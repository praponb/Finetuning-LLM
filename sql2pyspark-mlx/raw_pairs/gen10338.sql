SELECT
    s.id,
    s.supplier_name,
    s.country,
    t.stock_qty_range,
    t.total_stock_qty
FROM suppliers s
INNER JOIN (SELECT supplier_id, MAX(stock_qty) - MIN(stock_qty) AS stock_qty_range, SUM(stock_qty) AS total_stock_qty
    FROM products
    WHERE category = 'Sports'
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.stock_qty_range >= 21
