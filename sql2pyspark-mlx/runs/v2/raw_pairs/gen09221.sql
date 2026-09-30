SELECT
    i.id,
    i.last_restock,
    p.supplier_id,
    s.rating,
    s.city,
    ARRAY_JOIN(p.tags, '|') AS tags_text
FROM inventory i
FULL OUTER JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
ORDER BY i.last_restock DESC
