SELECT
    inventory.id,
    inventory.product_id,
    products.launch_date,
    products.brand,
    suppliers.country,
    suppliers.city
FROM inventory
LEFT JOIN products ON inventory.product_id = products.id
INNER JOIN suppliers ON products.supplier_id = suppliers.id
ORDER BY brand
