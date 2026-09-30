SELECT CASE WHEN amount >= 1260 THEN 'high' WHEN amount >= 660 THEN 'mid' ELSE 'low' END amount_bucket, MIN(amount) min_amount
FROM payments
WHERE HOUR(paid_at) BETWEEN 6 AND 12 AND TO_DATE(paid_at) < '2024-04-12'
GROUP BY CASE WHEN amount >= 1260 THEN 'high' WHEN amount >= 660 THEN 'mid' ELSE 'low' END
ORDER BY min_amount;
