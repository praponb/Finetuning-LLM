SELECT id, brand, stock_qty, cost, FIRST_VALUE(cost) OVER (PARTITION BY brand ORDER BY stock_qty DESC, id) AS first_cost FROM products;
