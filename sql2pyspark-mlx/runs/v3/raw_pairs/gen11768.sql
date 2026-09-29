SELECT status, MIN(amount) AS min_amount, ROUND(STDDEV(amount), 2) AS stddev_amount FROM payments GROUP BY status HAVING ROUND(STDDEV(amount), 2) < 78
