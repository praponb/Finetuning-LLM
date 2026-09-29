SELECT status, ROUND(STDDEV(amount), 2) AS stddev_amount, COUNT(amount) AS amount_count
FROM payments
WHERE method = 'cash'
GROUP BY status
HAVING ROUND(STDDEV(amount), 2) < 34;
