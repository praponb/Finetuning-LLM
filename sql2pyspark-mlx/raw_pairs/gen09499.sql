SELECT brand, SUM(price) AS total_price, MAX(stock_qty) AS max_stock_qty FROM products GROUP BY 1 HAVING MAX(stock_qty) < 340 ORDER BY brand DESC, max_stock_qty DESC;
