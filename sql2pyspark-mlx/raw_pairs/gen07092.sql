SELECT
    i.last_restock,
    p.category,
    p.stock_qty,
    s.country,
    s.city,
    CEIL(i.quantity) AS quantity_whole
FROM inventory i
JOIN products p ON i.product_id = p.id
LEFT OUTER JOIN suppliers s ON s.id = p.supplier_id
ORDER BY quantity_whole DESC
