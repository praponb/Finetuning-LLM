WITH numbered AS (SELECT id, category, cost, stock_qty, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) rn FROM products) SELECT * FROM numbered WHERE rn <= 1;
