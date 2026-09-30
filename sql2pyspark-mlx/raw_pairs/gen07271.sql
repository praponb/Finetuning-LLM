SELECT
    s.city,
    s.country,
    src.brand,
    COUNT(*) AS cnt,
    ROUND(SUM(src.stock_qty), 2) AS total_stock_qty
FROM products src
LEFT JOIN suppliers s ON src.supplier_id = s.id
GROUP BY GROUPING SETS ((s.city, s.country, src.brand), (s.city), ())
ORDER BY total_stock_qty NULLS LAST
