SELECT
    city,
    MONTH(opened_date) AS opened_month,
    SUM(CASE WHEN city = 'New York' THEN sqft ELSE 0 END) AS new_york_sqft,
    SUM(CASE WHEN region = 'West' THEN sqft ELSE 0 END) AS west_sqft,
    MAX(opened_date) AS last_opened_date
FROM stores
WHERE store_name LIKE 'D%'
GROUP BY city, MONTH(opened_date)
ORDER BY new_york_sqft DESC
