SELECT CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END rating_bucket, SUM(rating) total_rating, ROUND(SUM(rating) * 100.0 / SUM(SUM(rating)) OVER (), 2) AS pct_of_total
FROM suppliers
GROUP BY CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END
ORDER BY total_rating DESC, pct_of_total DESC, rating_bucket
LIMIT 10
