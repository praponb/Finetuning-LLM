WITH ranked AS (SELECT id, category, launch_date, product_name, ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date DESC, id) AS pos FROM products) SELECT * FROM ranked WHERE pos <= 5
