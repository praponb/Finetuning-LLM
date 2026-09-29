SELECT * FROM (SELECT CASE WHEN rating >= 4.2 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, city, rating FROM suppliers)
PIVOT (MAX(rating) FOR city IN ('London', 'Singapore'));
