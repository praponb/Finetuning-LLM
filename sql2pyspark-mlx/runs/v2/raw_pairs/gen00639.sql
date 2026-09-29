SELECT city, pct_smb, city_values
FROM (
    SELECT city, ROUND((100 * SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_smb, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
    FROM customers
    GROUP BY city
) AS s
WHERE pct_smb >= 59;
