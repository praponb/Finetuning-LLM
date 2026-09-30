SELECT QUARTER(paid_at) AS paid_quarter, ROUND(AVG(amount), 2) AS avg_amount
FROM payments
WHERE order_id IS NOT NULL AND amount BETWEEN 660 AND 1280 AND status = 'SUCCESS'
GROUP BY paid_quarter
HAVING ROUND(AVG(amount), 2) < 25
ORDER BY avg_amount, paid_quarter DESC
