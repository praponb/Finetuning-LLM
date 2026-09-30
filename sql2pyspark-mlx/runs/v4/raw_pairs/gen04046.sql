SELECT oi.id, oi.unit_price, o.order_date, o.amount, p.supplier_id FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id INNER JOIN products p ON oi.product_id = p.id;
