SELECT
    supplier_id,
    COUNT(stock_qty) AS stock_qty_count,
    MIN(stock_qty) AS min_stock_qty,
    MIN(cost) AS min_cost
FROM products
GROUP BY supplier_id;
