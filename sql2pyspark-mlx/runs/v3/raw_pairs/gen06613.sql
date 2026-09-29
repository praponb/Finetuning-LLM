SELECT oi.order_id, p.price, p.supplier_id, o.order_date FROM order_items oi INNER JOIN products p ON p.id = oi.product_id LEFT JOIN orders o ON oi.order_id = o.id
