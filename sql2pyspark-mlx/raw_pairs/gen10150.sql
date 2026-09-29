SELECT
    i.id,
    i.quantity,
    i.product_id,
    s.sqft,
    s.region,
    p.product_name,
    p.cost,
    TRIM(p.product_name) clean_product_name
FROM inventory i
JOIN stores s ON s.id = i.store_id
JOIN products p ON p.id = i.product_id
WHERE i.last_restock BETWEEN '2023-09-01' AND '2024-05-09' AND p.cost <= 610
