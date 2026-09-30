SELECT
    s.id,
    COUNT(p.id) AS product_count,
    SUM(CASE WHEN p.category = 'Books' THEN p.cost ELSE 0 END) AS books_cost,
    COUNT(p.stock_qty) AS stock_qty_count
FROM suppliers s
JOIN products p ON s.id = p.supplier_id
GROUP BY s.id;
