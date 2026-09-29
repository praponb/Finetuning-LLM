SELECT
    CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END rating_bucket,
    COUNT(rating) rating_count,
    COUNT(CASE WHEN city = 'London' THEN 1 END) london_count,
    ROUND(STDDEV(rating), 2) stddev_rating
FROM suppliers
WHERE country = 'UK' AND supplier_name IS NULL
GROUP BY 1
HAVING ROUND(STDDEV(rating), 2) >= 30
ORDER BY rating_bucket, london_count DESC
LIMIT 10
