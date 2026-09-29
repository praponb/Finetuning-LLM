SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY category ORDER BY price DESC) pos FROM products) r WHERE pos = 1;
