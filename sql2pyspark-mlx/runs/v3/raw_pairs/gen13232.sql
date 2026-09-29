SELECT
    region,
    city,
    MAX(opened_date) AS latest_opened_date,
    ROUND(AVG(sqft), 2) AS avg_sqft
FROM stores
GROUP BY region, city
ORDER BY latest_opened_date DESC, avg_sqft DESC;
