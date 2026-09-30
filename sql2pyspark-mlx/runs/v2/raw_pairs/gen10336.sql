SELECT * FROM (SELECT id, category, price, stock_qty, RANK() OVER (PARTITION BY category ORDER BY price) AS pos FROM products) AS ranked WHERE pos = 1
