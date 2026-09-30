SELECT method, SUM(amount) AS total_amount, ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total
FROM payments
WHERE method IN ('promptpay', 'bank_transfer')
GROUP BY method
ORDER BY method DESC, total_amount DESC
