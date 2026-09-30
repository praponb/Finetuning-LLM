WITH top_products AS (SELECT id, category, price, brand, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) AS rn FROM products) SELECT * FROM top_products WHERE rn <= 2 ORDER BY brand
