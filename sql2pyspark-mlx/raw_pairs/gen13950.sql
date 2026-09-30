SELECT status, CASE WHEN amount >= 1580 THEN 'high' WHEN amount >= 690 THEN 'mid' ELSE 'low' END amount_bucket, SUM(amount) total_amount
FROM payments
GROUP BY CUBE(status, CASE WHEN amount >= 1580 THEN 'high' WHEN amount >= 690 THEN 'mid' ELSE 'low' END)
ORDER BY status;
