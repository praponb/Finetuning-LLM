WITH ranked_products AS (SELECT *, DENSE_RANK() OVER (PARTITION BY supplier_id ORDER BY launch_date DESC) AS pos FROM products) SELECT * FROM ranked_products WHERE pos <= 3 ORDER BY id
