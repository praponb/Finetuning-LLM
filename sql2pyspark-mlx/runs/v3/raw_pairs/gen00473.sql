SELECT
    i.store_id,
    p.supplier_id,
    s.supplier_name,
    CEIL(p.price) AS price_whole
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
ORDER BY i.store_id DESC;
