WITH ranked_products AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC, id) AS rn FROM products) SELECT id, brand, stock_qty FROM ranked_products WHERE rn = 1;
