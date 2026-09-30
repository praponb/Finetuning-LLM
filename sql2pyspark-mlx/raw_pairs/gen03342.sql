SELECT CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 2.6 THEN 'mid' ELSE 'low' END rating_bucket, SUM(rating) total_rating, SUM(SUM(rating)) OVER (ORDER BY CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 2.6 THEN 'mid' ELSE 'low' END) AS cumulative_rating
FROM suppliers
GROUP BY CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 2.6 THEN 'mid' ELSE 'low' END;
