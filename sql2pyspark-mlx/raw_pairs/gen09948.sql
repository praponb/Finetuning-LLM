SELECT
    i.id,
    i.last_restock,
    p.brand,
    s.supplier_name,
    s.country
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id;
