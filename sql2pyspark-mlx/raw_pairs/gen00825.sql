SELECT
    CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 1.8 THEN 'mid' ELSE 'low' END rating_bucket,
    COUNT(*) cnt,
    ROUND(AVG(rating), 2) avg_rating,
    COUNT(CASE WHEN city = 'London' THEN 1 END) london_count
FROM suppliers
WHERE rating >= 3.0 AND (country = 'TH' OR country IN ('SG', 'TH', 'JP'))
GROUP BY 1
ORDER BY cnt DESC, rating_bucket
LIMIT 50
