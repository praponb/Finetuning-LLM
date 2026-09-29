SELECT
    CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket,
    SUM(rating) AS total_rating,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(rating) DESC) AS rating_rank
FROM suppliers
GROUP BY CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END
ORDER BY rating_rank;
