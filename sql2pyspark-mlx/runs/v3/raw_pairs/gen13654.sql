SELECT * FROM (SELECT category, MIN(stock_qty) AS min_stock_qty FROM products WHERE category = 'Electronics' GROUP BY category) AS x WHERE min_stock_qty >= 220 ORDER BY category DESC;
