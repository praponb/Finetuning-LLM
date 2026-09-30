SELECT brand, MAX(stock_qty) max_stock_qty, SUM(CASE WHEN category = 'Sports' THEN 1 ELSE 0 END) sports_count FROM products GROUP BY brand ORDER BY max_stock_qty DESC, brand LIMIT 10
