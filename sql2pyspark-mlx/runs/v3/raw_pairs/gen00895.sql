SELECT o.id, o.shipping_fee, o.customer_id, c.credit_limit, c.city, s.city AS store_city FROM orders o INNER JOIN customers c ON o.customer_id = c.id LEFT JOIN stores s ON o.store_id = s.id
