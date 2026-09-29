SELECT
    i.last_restock,
    p.stock_qty,
    s.rating,
    SIZE(p.tags) AS tags_count
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON s.id = p.supplier_id
ORDER BY rating DESC;
