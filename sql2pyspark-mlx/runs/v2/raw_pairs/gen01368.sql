SELECT status, ROUND(STDDEV(amount), 2) AS stddev_amount, COUNT(amount) AS amount_count, SUM(amount) AS total_amount FROM payments GROUP BY status ORDER BY status, total_amount;
