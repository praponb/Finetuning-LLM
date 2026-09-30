SELECT CASE WHEN amount >= 1400 THEN 'high' WHEN amount >= 750 THEN 'mid' ELSE 'low' END AS amount_bucket, COUNT(*) AS total_count FROM orders GROUP BY 1;
