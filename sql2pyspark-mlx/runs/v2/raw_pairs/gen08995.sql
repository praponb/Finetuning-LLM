SELECT id, supplier_id, launch_date, DENSE_RANK() OVER (PARTITION BY supplier_id ORDER BY launch_date) AS dense_rnk FROM products;
