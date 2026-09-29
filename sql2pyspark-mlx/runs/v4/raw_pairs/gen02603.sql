SELECT amount_bucket, status, amount_count
FROM (
    SELECT CASE WHEN amount >= 1270 THEN 'high' WHEN amount >= 720 THEN 'mid' ELSE 'low' END AS amount_bucket, status, COUNT(amount) AS amount_count
    FROM payments
    GROUP BY CASE WHEN amount >= 1270 THEN 'high' WHEN amount >= 720 THEN 'mid' ELSE 'low' END, status
) AS sub
WHERE amount_count >= 8;
