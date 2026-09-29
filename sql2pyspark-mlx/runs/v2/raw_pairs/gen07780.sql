SELECT * FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY sale_date DESC) AS row_num FROM store_sales WHERE sale_date >= '2025-04-01') AS r WHERE row_num = 1 ORDER BY row_num
