SELECT
    p.city,
    src.brand,
    COUNT(*) AS rows_total,
    SUM(src.price) AS total_price
FROM products src
INNER JOIN suppliers p ON src.supplier_id = p.id
GROUP BY ROLLUP (p.city, src.brand)
