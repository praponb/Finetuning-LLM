WITH ranked_products AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price, id) AS row_num FROM products) SELECT * FROM ranked_products WHERE row_num <= 3
