SELECT
    oi.id,
    oi.unit_price,
    oi.order_id,
    oi.product_id
FROM order_items oi
WHERE oi.product_id NOT IN (SELECT id FROM products WHERE category IN ('Home', 'Books'));
