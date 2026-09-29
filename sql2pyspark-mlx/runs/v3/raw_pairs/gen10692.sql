SELECT id, brand, stock_qty, price FROM (SELECT *, RANK() OVER (PARTITION BY brand ORDER BY stock_qty DESC) AS rn FROM products) AS ranked WHERE rn <= 3 ORDER BY price DESC;
