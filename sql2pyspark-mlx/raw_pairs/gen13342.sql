SELECT status, ROUND((100 * SUM(CASE WHEN method = 'promptpay' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_promptpay
FROM payments
GROUP BY status
HAVING pct_promptpay < 34;
