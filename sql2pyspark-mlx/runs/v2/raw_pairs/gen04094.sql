SELECT ROUND(STDDEV(amount), 2) AS stddev_amount, SUM(amount) AS total_amount, COUNT(amount) AS amount_count FROM payments ORDER BY amount_count DESC
