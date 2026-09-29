SELECT id, stock_qty, ROUND(stock_qty / cost, 2) AS stock_qty_per_cost
FROM products;
