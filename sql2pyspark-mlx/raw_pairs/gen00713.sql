SELECT DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month, MAX(amount) - MIN(amount) AS amount_range, MIN(amount) AS min_amount
FROM payments
WHERE (method = 'paypal' AND amount BETWEEN 1290 AND 1330) OR method = 'card'
GROUP BY 1
HAVING amount_range >= 71
