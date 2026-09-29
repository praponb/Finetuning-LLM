SELECT
    p.id,
    p.brand,
    p.price,
    p.launch_date
FROM products p
WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id)
ORDER BY id
