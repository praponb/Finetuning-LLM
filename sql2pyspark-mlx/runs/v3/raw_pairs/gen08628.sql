SELECT
    CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket,
    SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END) AS de_count,
    COUNT(*) AS total_count,
    MIN(rating) AS min_rating
FROM suppliers
WHERE rating < 3.8
GROUP BY rating_bucket
HAVING SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END) > 76
ORDER BY rating_bucket
