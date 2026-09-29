WITH top_products AS (SELECT id, brand, price, DENSE_RANK() OVER (PARTITION BY brand ORDER BY price DESC) pos FROM products WHERE product_name IS NOT NULL) SELECT * FROM top_products WHERE pos = 1
