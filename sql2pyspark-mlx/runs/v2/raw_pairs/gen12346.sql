SELECT
    p.id,
    p.category,
    p.product_name,
    p.stock_qty,
    s.supplier_name,
    s.country
FROM products p
JOIN suppliers s ON s.id = p.supplier_id
ORDER BY country;
