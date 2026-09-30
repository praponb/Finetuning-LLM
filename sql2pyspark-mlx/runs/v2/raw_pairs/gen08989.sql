SELECT product_name, ROUND(stock_qty / cost, 2) AS stock_qty_per_cost FROM products WHERE product_name IS NOT NULL ORDER BY product_name ASC, stock_qty_per_cost DESC
