SELECT method, ROUND(STDDEV(amount), 2) AS stddev_amount FROM payments GROUP BY method;
