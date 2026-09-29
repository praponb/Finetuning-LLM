SELECT brand, AVG(cost) AS avg_cost, MAX(stock_qty) AS max_stock_qty FROM products GROUP BY brand;
