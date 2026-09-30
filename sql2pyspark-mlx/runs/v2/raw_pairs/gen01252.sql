SELECT brand, category, MIN(stock_qty) min_stock_qty FROM products GROUP BY brand, category ORDER BY category
