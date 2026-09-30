SELECT id, category, stock_qty, SUM(stock_qty) OVER (PARTITION BY category) category_sum_stock_qty FROM products;
