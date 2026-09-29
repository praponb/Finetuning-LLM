SELECT
    CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END rating_bucket,
    city,
    MIN(rating) min_rating,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) uk_count
FROM suppliers
GROUP BY CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END, city
ORDER BY min_rating DESC, rating_bucket ASC, city ASC
LIMIT 3
