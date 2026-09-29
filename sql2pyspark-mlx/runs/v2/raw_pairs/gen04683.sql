SELECT method, ROUND((100 * SUM(CASE WHEN method = 'promptpay' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_promptpay, AVG(amount) avg_amount
FROM payments
WHERE amount < 610
GROUP BY 1
