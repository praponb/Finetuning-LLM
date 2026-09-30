SELECT
    QUARTER(opened_date) AS opened_quarter,
    city,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    SUM(sqft) AS total_sqft,
    SUM(CASE WHEN city = 'Bangkok' THEN sqft ELSE 0 END) AS bangkok_sqft
FROM stores
WHERE opened_date BETWEEN '2023-09-14' AND '2024-05-04' AND store_name LIKE 'R%'
GROUP BY 1, 2
HAVING bangkok_sqft > 43
ORDER BY city ASC, opened_quarter ASC
LIMIT 5
