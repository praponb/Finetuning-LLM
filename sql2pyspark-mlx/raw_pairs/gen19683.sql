SELECT
    i.id,
    i.store_id,
    p.product_name,
    p.supplier_id,
    s.city
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.cost > 660 AND s.country NOT IN ('JP', 'TH')
