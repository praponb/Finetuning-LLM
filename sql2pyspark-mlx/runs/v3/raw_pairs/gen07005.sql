SELECT CASE WHEN amount >= 1440 THEN 'high' WHEN amount >= 310 THEN 'mid' ELSE 'low' END AS amount_bucket, SUM(amount) AS total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
FROM payments
GROUP BY CASE WHEN amount >= 1440 THEN 'high' WHEN amount >= 310 THEN 'mid' ELSE 'low' END
