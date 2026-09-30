SELECT
    MONTH(opened_date) AS opened_month,
    city,
    COUNT(DISTINCT sqft) AS unique_regions,
    ROUND((100 * SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_bangkok
FROM stores
GROUP BY opened_month, city
ORDER BY opened_month, city
LIMIT 20
