SELECT DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month, ROUND(AVG(amount), 2) AS avg_amount, MIN(amount) AS min_amount
FROM payments
WHERE method = 'cash' AND method = 'cash'
GROUP BY DATE_FORMAT(paid_at, 'yyyy-MM')
ORDER BY min_amount
