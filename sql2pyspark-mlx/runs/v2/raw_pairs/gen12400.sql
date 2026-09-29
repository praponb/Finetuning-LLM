SELECT method, ROUND(STDDEV(amount), 2) AS stddev_amount FROM payments GROUP BY method ORDER BY method, stddev_amount DESC LIMIT 1;
