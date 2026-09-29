SELECT id, brand, cost, product_name, FIRST_VALUE(product_name) OVER (PARTITION BY brand ORDER BY cost DESC, id) AS first_product_name FROM products ORDER BY brand ASC
