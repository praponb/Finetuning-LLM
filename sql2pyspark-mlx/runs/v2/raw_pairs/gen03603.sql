SELECT s.cost, o.shipping_fee, c.age, c.credit_limit, CAST(s.cost AS INT) AS cost_int FROM shipments s LEFT JOIN orders o ON s.order_id = o.id INNER JOIN customers c ON o.customer_id = c.id
