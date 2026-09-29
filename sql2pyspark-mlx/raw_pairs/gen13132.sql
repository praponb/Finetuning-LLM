SELECT
    region,
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
    ROUND(STDDEV(sqft), 2) AS stddev_sqft,
    MAX(sqft) - MIN(sqft) AS sqft_range,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count
FROM stores
GROUP BY 1, 2
HAVING ROUND(STDDEV(sqft), 2) > 78;
