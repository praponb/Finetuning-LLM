SELECT
    p.id,
    p.cost,
    p.launch_date,
    p.brand
FROM products p
WHERE p.id IN (SELECT product_id FROM order_items)
ORDER BY brand, id
LIMIT 10;
