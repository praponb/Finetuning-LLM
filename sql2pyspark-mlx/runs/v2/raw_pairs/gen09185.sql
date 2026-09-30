SELECT
    CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket,
    SUM(rating) AS total_rating,
    ROUND(AVG(rating), 2) AS avg_rating,
    ROUND((100 * SUM(CASE WHEN country = 'JP' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_jp
FROM suppliers
GROUP BY CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END
ORDER BY pct_jp;
