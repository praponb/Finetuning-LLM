SELECT CASE WHEN amount >= 1250 THEN 'high' WHEN amount >= 720 THEN 'mid' ELSE 'low' END AS amount_bucket, method, ROUND(AVG(amount), 2) AS avg_amount
FROM payments
WHERE method IS NOT NULL AND (amount >= 530 OR amount > 620)
GROUP BY CASE WHEN amount >= 1250 THEN 'high' WHEN amount >= 720 THEN 'mid' ELSE 'low' END, method;
