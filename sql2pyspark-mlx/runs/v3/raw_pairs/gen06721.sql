SELECT id, status, customer_id, discount, SUM(discount) OVER (PARTITION BY status, customer_id) status_sum_discount FROM orders WHERE discount < 0.37 ORDER BY discount DESC;
