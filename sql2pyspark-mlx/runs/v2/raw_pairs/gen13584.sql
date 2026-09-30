SELECT
    p.id,
    p.brand,
    p.price,
    p.category
FROM products p
WHERE p.id IN (SELECT product_id FROM order_items)
