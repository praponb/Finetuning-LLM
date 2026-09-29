SELECT id, category, stock_qty, brand FROM (SELECT id, category, stock_qty, brand, ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty DESC, id) rnk FROM products) r WHERE rnk <= 3;
