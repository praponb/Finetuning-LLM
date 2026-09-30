SELECT id, category, launch_date, NTILE(2) OVER (PARTITION BY category ORDER BY launch_date, id) AS bucket FROM products ORDER BY id DESC;
