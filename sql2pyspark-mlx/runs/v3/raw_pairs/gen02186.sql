SELECT method, order_id, ROUND(STDDEV(amount), 2) AS stddev_amount, COUNT(*) AS row_count FROM payments GROUP BY method, order_id ORDER BY row_count DESC;
