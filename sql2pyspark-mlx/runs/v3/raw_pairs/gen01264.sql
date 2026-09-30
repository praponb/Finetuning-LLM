SELECT o.id, o.customer_id, o.shipping_fee, c.signup_date FROM orders o INNER JOIN customers c ON o.customer_id = c.id;
