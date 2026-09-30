SELECT HOUR(paid_at) AS paid_hour, COUNT(amount) AS amount_count, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM payments
WHERE amount <= 660
GROUP BY 1
