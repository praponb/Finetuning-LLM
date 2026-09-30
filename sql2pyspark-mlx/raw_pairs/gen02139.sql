SELECT ROUND(AVG(stock_qty), 2) AS avg_stock_qty, SUM(CASE WHEN brand = 'Initech' THEN stock_qty ELSE 0 END) AS initech_stock_qty FROM products ORDER BY avg_stock_qty DESC;
