SELECT
    CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END rating_bucket,
    city,
    ROUND(AVG(rating), 2) avg_rating,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) jp_count
FROM suppliers
GROUP BY CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END, city
ORDER BY 4 DESC, city
