SELECT CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.8 THEN 'mid' ELSE 'low' END rating_bucket, COUNT(rating) rating_count FROM suppliers GROUP BY rating_bucket;
