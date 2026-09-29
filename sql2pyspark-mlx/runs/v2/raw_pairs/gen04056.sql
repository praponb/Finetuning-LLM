SELECT oi.id, oi.unit_price, p.category, p.brand FROM order_items oi LEFT JOIN products p ON oi.product_id = p.id WHERE oi.order_id IN (8, 34)
