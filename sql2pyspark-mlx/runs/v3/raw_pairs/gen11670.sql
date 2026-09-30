SELECT
    i.id,
    i.store_id,
    p.price,
    s.sqft
FROM inventory i
RIGHT JOIN products p ON p.id = i.product_id
FULL OUTER JOIN stores s ON i.store_id = s.id
WHERE NOT (p.supplier_id IS NOT NULL AND s.store_name NOT LIKE '%Riverside%')
ORDER BY id DESC;
