WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS row_num FROM stores) SELECT * FROM ranked WHERE row_num = 1
