SELECT brand, MAX(cost) - MIN(cost) AS cost_range FROM products GROUP BY brand HAVING SUM(stock_qty) < 700 ORDER BY brand LIMIT 20
