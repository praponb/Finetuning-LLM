WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty DESC, id) AS row_num FROM products) SELECT id, category, stock_qty, launch_date FROM ranked WHERE row_num <= 1
