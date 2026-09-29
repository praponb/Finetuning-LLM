WITH top_products AS (SELECT id, brand, stock_qty, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty, id) AS rn FROM products) SELECT id, brand, stock_qty FROM top_products WHERE rn <= 2;
