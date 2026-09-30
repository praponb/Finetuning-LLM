SELECT city, country, pct_bangkok
FROM (
    SELECT city, country, ROUND((100 * SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_bangkok
    FROM suppliers
    GROUP BY city, country
) x
WHERE pct_bangkok >= 67
ORDER BY 1 DESC
