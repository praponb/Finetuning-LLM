SELECT
    i.id,
    i.store_id,
    i.product_id,
    p.supplier_id,
    p.cost,
    s.opened_date,
    s.sqft,
    TRIM(s.store_name) AS clean_store_name
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
ORDER BY supplier_id
