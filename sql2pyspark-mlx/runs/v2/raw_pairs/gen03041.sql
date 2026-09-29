SELECT method, ROUND(STDDEV(amount), 2) AS stddev_amount, MIN(amount) AS min_amount FROM payments GROUP BY method ORDER BY stddev_amount
