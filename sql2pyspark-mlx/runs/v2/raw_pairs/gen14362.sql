WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY brand ORDER BY stock_qty DESC) AS rnk FROM products) SELECT id, brand, stock_qty, product_name FROM numbered WHERE rnk = 1 ORDER BY brand;
