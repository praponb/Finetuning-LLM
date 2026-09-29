SELECT
    country,
    city,
    CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END rating_bucket,
    ROUND(STDDEV(rating), 2) stddev_rating
FROM suppliers
GROUP BY 1, 2, 3
