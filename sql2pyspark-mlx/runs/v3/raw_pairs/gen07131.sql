WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY supplier_id ORDER BY launch_date) AS pos FROM products WHERE cost IS NULL) SELECT * FROM ranked WHERE pos <= 1 ORDER BY pos;
