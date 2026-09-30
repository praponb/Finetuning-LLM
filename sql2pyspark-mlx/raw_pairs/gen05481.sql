SELECT oi.id, oi.unit_price, o.amount FROM order_items oi LEFT JOIN orders o ON oi.order_id = o.id WHERE o.discount > 0.16 AND oi.unit_price IS NOT NULL;
