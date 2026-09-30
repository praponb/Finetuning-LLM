SELECT account_id, is_flagged, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
FROM transactions
GROUP BY account_id, is_flagged
ORDER BY txn_type_values, account_id, is_flagged
LIMIT 10
