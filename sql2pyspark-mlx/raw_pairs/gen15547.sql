SELECT method, MAX(amount) - MIN(amount) AS amount_range, ROUND(STDDEV(amount), 2) AS stddev_amount FROM payments GROUP BY 1;
