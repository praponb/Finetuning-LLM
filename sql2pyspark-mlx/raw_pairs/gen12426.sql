SELECT
    d.country,
    d.city,
    src.brand,
    COUNT(*) AS rows_total,
    MAX(src.stock_qty) AS max_stock_qty
FROM products src
LEFT JOIN suppliers d ON src.supplier_id = d.id
GROUP BY d.country, d.city, src.brand;
