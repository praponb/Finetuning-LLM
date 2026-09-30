SELECT
    p.launch_date,
    p.supplier_id,
    p.product_name,
    i.quantity,
    i.product_id,
    i.store_id
FROM products p
LEFT JOIN inventory i ON p.id = i.product_id
WHERE i.product_id = 4
ORDER BY p.supplier_id DESC
