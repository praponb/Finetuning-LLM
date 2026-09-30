SELECT is_flagged, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
FROM transactions
WHERE account_id IN (19, 20, 35) OR merchant = 'Shell'
GROUP BY 1
ORDER BY 2
