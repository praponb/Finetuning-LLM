SELECT o.id, o.amount, o.shipping_fee, c.credit_limit, c.name, c.segment FROM orders o INNER JOIN customers c ON c.id = o.customer_id
