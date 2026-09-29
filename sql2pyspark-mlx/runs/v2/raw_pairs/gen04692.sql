SELECT txn_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values FROM transactions WHERE merchant = 'Grab' OR is_flagged GROUP BY txn_type ORDER BY merchant_values;
