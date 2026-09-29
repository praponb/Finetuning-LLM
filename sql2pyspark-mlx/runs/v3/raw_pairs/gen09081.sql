SELECT id, category, stock_qty FROM (SELECT id, category, stock_qty, RANK() OVER (PARTITION BY category ORDER BY stock_qty) AS rn FROM products) AS r WHERE rn <= 5 ORDER BY category ASC;
