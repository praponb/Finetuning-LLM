SELECT merchant, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list FROM transactions GROUP BY merchant ORDER BY merchant ASC
