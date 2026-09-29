SELECT
    dim.city,
    s.brand,
    COUNT(*) AS num_rows,
    MAX(s.cost) AS max_cost,
    AVG(s.stock_qty) AS avg_stock_qty,
    GROUPING_ID() AS gid
FROM products s
LEFT JOIN suppliers dim ON s.supplier_id = dim.id
GROUP BY GROUPING SETS ((dim.city, s.brand), (dim.city))
ORDER BY avg_stock_qty DESC NULLS FIRST;
