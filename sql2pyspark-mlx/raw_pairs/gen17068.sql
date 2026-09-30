SELECT
    i.id,
    i.product_id,
    p.supplier_id,
    p.product_name
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
WHERE p.launch_date <= '2024-05-20';
