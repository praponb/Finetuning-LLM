SELECT p.paid_at, o.shipping_fee, c.country, c.segment FROM payments p INNER JOIN orders o ON p.order_id = o.id LEFT JOIN customers c ON c.id = o.customer_id
