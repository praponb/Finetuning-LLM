SELECT
    city,
    CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END rating_bucket,
    ROUND(STDDEV(rating), 2) stddev_rating,
    COUNT(rating) rating_count
FROM suppliers
WHERE country IN ('TH', 'SG', 'JP') AND supplier_name IS NOT NULL
GROUP BY 1, 2
ORDER BY rating_bucket DESC
