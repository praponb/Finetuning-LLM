SELECT
    i.id,
    i.last_restock,
    i.quantity,
    s.sqft,
    s.region,
    p.category,
    p.stock_qty,
    INITCAP(TRIM(s.store_name)) AS store_name_title
FROM inventory i
FULL OUTER JOIN stores s ON s.id = i.store_id
INNER JOIN products p ON i.product_id = p.id
WHERE p.brand = 'Initech'
