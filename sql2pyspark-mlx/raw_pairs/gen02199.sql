SELECT
    CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket,
    MAX(rating) AS max_rating,
    SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count,
    COUNT(*) AS total_count
FROM suppliers
WHERE supplier_name LIKE 'P%' AND country = 'DE'
GROUP BY CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END
ORDER BY rating_bucket
