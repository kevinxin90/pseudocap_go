import os
import csv
from biothings_client import get_client

def batch_query_entrez_from_locus_tag(locus_tag_list):
    """ convert a list of locus tags to list of entrez ids
    Keyword arguments:
    locus_tag_list: a list of locus tags
    """
    mapping_dict = {}
    id_list = list(set(locus_tag_list))
    # initiate the mydisease.info python client
    client = get_client('gene')
    params = ','.join(locus_tag_list)
    res = client.querymany(params, scopes="locus_tag", fields="_id")
    for _doc in res:
        if '_id' not in _doc:
            print('can not convert', _doc)
        mapping_dict[_doc['query']] = _doc['_id'] if '_id' in _doc else _doc["query"]
    return mapping_dict

def load_data(data_folder):
    f_path = os.path.join(data_folder, 'gene_association.pseudocap')
    with open(f_path) as f:
        freader = csv.reader(f, delimiter="\t")
        for i in range(8):
            next(freader)
        tmp = {}
        for line in freader:
            locus_tag = line[1]
            if not locus_tag in tmp:
                tmp[locus_tag] = []
            rec = {
                "id": line[4],
                "pubmed": line[5]
            }
            tmp[locus_tag].append(rec)
        locus_tags = tmp.keys()
        id_mapping = batch_query_entrez_from_locus_tag(locus_tags)
        for k, v in tmp.items():
            doc = {
                "_id": id_mapping.get(k, k),
                "locus_tag": k,
                "go": v
            }
            yield doc
