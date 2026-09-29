SELECT o.id, o.customer_id, o.store_id, o.amount, c.city FROM orders o INNER JOIN customers c ON o.customer_id = c.id WHERE c.signup_date < '2025-03-01' AND o.shipping_fee BETWEEN 21 AND 25
