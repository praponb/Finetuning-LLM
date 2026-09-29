SELECT oi.id, oi.order_id, oi.product_id, p.supplier_id, o.store_id, o.status FROM order_items oi INNER JOIN products p ON oi.product_id = p.id INNER JOIN orders o ON oi.order_id = o.id
