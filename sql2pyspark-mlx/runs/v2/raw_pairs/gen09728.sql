SELECT
    i.store_id,
    i.last_restock,
    p.supplier_id,
    s.rating,
    s.country
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
INNER JOIN suppliers s ON p.supplier_id = s.id
ORDER BY store_id ASC;
