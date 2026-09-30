SELECT oi.id, oi.order_id, oi.unit_price, p.price FROM order_items oi LEFT JOIN products p ON p.id = oi.product_id WHERE p.price IS NOT NULL ORDER BY id DESC;
