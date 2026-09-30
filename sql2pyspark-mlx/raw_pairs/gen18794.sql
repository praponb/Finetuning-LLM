SELECT AVG(stock_qty) AS avg_stock_qty, SUM(CASE WHEN category = 'Grocery' THEN cost ELSE 0 END) AS grocery_cost, MAX(stock_qty) AS max_stock_qty FROM products
