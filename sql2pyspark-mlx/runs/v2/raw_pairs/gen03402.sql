SELECT COUNT(*) AS product_count, MAX(stock_qty) AS max_stock_qty, SUM(cost) AS total_cost
FROM products
