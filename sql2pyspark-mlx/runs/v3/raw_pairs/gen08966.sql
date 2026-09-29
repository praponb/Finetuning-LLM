WITH top_products AS (SELECT id, brand, price, supplier_id, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC, id) AS rnk FROM products) SELECT * FROM top_products WHERE rnk <= 2
