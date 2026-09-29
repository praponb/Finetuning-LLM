SELECT status, MIN(paid_at) AS first_paid_at, ROUND(STDDEV(amount), 2) AS stddev_amount FROM payments GROUP BY status;
