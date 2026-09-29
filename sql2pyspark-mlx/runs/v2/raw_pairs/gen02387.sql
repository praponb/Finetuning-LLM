SELECT * FROM (SELECT id, brand, price, category, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC, id) AS pos FROM products WHERE product_name LIKE 'Co%') AS r WHERE pos <= 3;
