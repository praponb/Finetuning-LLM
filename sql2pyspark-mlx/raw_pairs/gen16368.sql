SELECT id, category, price, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) AS rn FROM products
