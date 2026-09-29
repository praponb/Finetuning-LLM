SELECT method, ROUND((100 * SUM(CASE WHEN method = 'promptpay' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_promptpay, SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) AS success_amount
FROM payments
WHERE status NOT IN ('SUCCESS', 'REFUNDED') AND (amount >= 1470 OR status != 'SUCCESS')
GROUP BY method
ORDER BY success_amount
