SELECT
    ss.id,
    ss.units,
    p.stock_qty,
    s.country,
    ss.revenue * ss.units AS revenue_per_unit
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE p.cost BETWEEN 290 AND 520
ORDER BY revenue_per_unit, id
LIMIT 10;
