SELECT *
FROM (
    SELECT country, MAX(rating) - MIN(rating) AS rating_range, ROUND((100 * SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_chiang_mai
    FROM suppliers
    GROUP BY country
) AS agg
WHERE pct_chiang_mai < 25
ORDER BY rating_range DESC, pct_chiang_mai DESC, country
LIMIT 10;
