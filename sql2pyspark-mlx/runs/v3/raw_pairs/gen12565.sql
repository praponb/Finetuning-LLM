SELECT id, category, stock_qty, MAX(stock_qty) OVER (PARTITION BY category) AS category_max_stock_qty FROM products WHERE LENGTH(product_name) < 6;
