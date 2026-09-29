SELECT id, status, order_date, discount, SUM(discount) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount FROM orders;
