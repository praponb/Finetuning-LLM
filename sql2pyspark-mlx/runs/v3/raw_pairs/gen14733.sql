SELECT
    CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket,
    AVG(rating) AS avg_rating,
    ROUND(STDDEV(rating), 2) AS stddev_rating,
    COUNT(*) AS cnt
FROM suppliers
GROUP BY CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END
ORDER BY cnt;
