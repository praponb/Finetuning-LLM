SELECT
    city,
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
    MIN(opened_date) AS min_opened_date,
    ROUND(STDDEV(sqft), 2) AS stddev_sqft
FROM stores
WHERE sqft = 13909
GROUP BY city, DATE_FORMAT(opened_date, 'yyyy-MM');
