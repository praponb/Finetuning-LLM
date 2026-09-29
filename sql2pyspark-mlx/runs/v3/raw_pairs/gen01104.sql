SELECT id, category, launch_date, ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date DESC, id) AS row_num FROM products;
