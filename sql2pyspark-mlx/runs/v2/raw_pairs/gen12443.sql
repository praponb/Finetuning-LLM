SELECT
    region,
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
    ROUND((100 * SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_london,
    ROUND(STDDEV(sqft), 2) AS stddev_sqft,
    ROUND((100 * SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_new_york
FROM stores
WHERE sqft > 6669 AND store_name LIKE 'O%'
GROUP BY region, opened_month
