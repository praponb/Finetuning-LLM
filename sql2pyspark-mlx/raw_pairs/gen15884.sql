SELECT status, method, ROUND(STDDEV(amount), 2) AS stddev_amount FROM payments GROUP BY status, method
