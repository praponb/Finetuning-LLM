SELECT MONTH(paid_at) AS paid_month, SUM(CASE WHEN method = 'paypal' THEN 1 ELSE 0 END) AS paypal_count
FROM payments
WHERE amount < 460
GROUP BY MONTH(paid_at);
