SELECT o.order_date, c.city, o.amount / NULLIF(o.discount, 0) AS amount_per_discount FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.store_id IN (25, 32) AND c.credit_limit > 5200;
