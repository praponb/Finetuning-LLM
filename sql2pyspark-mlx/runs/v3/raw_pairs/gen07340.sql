SELECT *
FROM (
    SELECT country, ROUND((100 * SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_bangkok, MIN(rating) AS min_rating
    FROM suppliers
    WHERE supplier_name LIKE '%City%'
    GROUP BY country
) AS agg
WHERE pct_bangkok < 62
