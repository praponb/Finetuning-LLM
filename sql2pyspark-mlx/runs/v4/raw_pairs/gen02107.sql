SELECT is_flagged, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values, SUM(CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END) AS 7_eleven_count
FROM transactions
WHERE is_flagged = TRUE
GROUP BY is_flagged;
