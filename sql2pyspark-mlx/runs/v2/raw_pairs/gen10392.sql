SELECT segment, pct_chiang_mai
FROM (
    SELECT segment, ROUND((100 * SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_chiang_mai
    FROM customers
    GROUP BY segment
) AS s
WHERE pct_chiang_mai >= 62
