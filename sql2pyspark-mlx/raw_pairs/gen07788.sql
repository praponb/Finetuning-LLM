SELECT CASE WHEN rating >= 3.9 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(*) AS total_count, SUM(CASE WHEN country = 'UK' THEN rating ELSE 0 END) AS uk_rating
FROM suppliers
GROUP BY CASE WHEN rating >= 3.9 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END
HAVING SUM(rating) >= 14;
