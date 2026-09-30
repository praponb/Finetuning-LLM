SELECT is_flagged, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values
FROM transactions
WHERE account_id IN (9, 20, 32) AND account_id IN (13, 19, 28) AND account_id IN (7, 13, 14, 20)
GROUP BY 1
