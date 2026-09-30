SELECT id, discount, status, FIRST_VALUE(status) OVER (ORDER BY discount, id) AS first_status FROM orders
