SELECT * FROM (SELECT id, brand, price, category, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price, id) AS rn FROM products WHERE stock_qty = 332) AS ranked WHERE rn = 1
