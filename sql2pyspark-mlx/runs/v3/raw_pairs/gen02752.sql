SELECT
    s.supplier_name,
    s.country,
    p.product_name,
    p.category,
    p.cost
FROM suppliers s
LEFT OUTER JOIN products p ON s.id = p.supplier_id
WHERE p.stock_qty < 287 AND s.rating >= 2.7
