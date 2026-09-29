SELECT
    i.last_restock,
    s.store_name,
    s.opened_date,
    p.category,
    p.supplier_id
FROM inventory i
FULL OUTER JOIN stores s ON s.id = i.store_id
RIGHT JOIN products p ON i.product_id = p.id
WHERE s.store_name NOT LIKE 'Air%'
ORDER BY p.supplier_id
