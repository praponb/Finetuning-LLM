SELECT SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END) AS smb_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, COUNT(DISTINCT age) AS unique_countrys
FROM customers
WHERE age <= 33
ORDER BY city_values
