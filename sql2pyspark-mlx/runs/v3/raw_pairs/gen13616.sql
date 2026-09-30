SELECT YEAR(paid_at) AS paid_year, ROUND((100 * SUM(CASE WHEN method = 'paypal' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_paypal
FROM payments
GROUP BY YEAR(paid_at)
ORDER BY pct_paypal DESC;
