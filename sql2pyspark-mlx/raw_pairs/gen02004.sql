SELECT is_flagged, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values FROM transactions GROUP BY 1 ORDER BY txn_type_values ASC
