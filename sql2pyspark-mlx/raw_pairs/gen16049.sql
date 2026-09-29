SELECT
    city,
    ROUND(STDDEV(sqft), 2) stddev_sqft,
    SUM(CASE WHEN region = 'Central' THEN 1 ELSE 0 END) central_count,
    MIN(opened_date) first_opened_date
FROM stores
GROUP BY 1
