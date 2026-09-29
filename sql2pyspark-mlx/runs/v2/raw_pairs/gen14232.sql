SELECT oi.id, oi.unit_price, oi.order_id
FROM order_items oi
WHERE oi.quantity = 4 AND oi.product_id NOT IN (SELECT id FROM products WHERE brand IN ('Initech', 'Umbrella'));
