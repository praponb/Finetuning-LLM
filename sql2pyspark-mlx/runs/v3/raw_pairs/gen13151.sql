SELECT
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    ROUND(STDDEV(sqft), 2) AS stddev_sqft,
    SUM(sqft) AS total_sqft
FROM stores
WHERE store_name NOT LIKE '%Centr%' OR store_name NOT LIKE '%Cen%'
GROUP BY 1
ORDER BY 3 DESC;
