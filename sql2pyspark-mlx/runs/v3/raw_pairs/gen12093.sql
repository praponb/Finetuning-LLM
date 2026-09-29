SELECT
    CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END rating_bucket,
    SUM(rating) total_rating,
    COUNT(*) cnt,
    SUM(SUM(rating)) OVER (ORDER BY CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END) AS cumulative_rating
FROM suppliers
WHERE country NOT IN ('JP', 'SG', 'US')
GROUP BY CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END
ORDER BY total_rating, cumulative_rating;
