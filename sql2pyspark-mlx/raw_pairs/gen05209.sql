SELECT id, brand, cost, stock_qty FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost, id) AS row_num FROM products WHERE cost IS NULL) AS r WHERE row_num <= 1 ORDER BY cost
