SELECT
    ss.store_id,
    p.launch_date,
    p.supplier_id,
    s.country
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE p.price <= 990 OR ss.units BETWEEN 21 AND 32
