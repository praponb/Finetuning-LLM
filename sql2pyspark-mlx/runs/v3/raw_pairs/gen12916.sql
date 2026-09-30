SELECT amount_bucket, total_count
FROM (
    SELECT CASE WHEN amount >= 1560 THEN 'high' WHEN amount >= 430 THEN 'mid' ELSE 'low' END AS amount_bucket, COUNT(*) AS total_count
    FROM orders
    GROUP BY CASE WHEN amount >= 1560 THEN 'high' WHEN amount >= 430 THEN 'mid' ELSE 'low' END
) AS s
WHERE total_count >= 8
ORDER BY amount_bucket;
