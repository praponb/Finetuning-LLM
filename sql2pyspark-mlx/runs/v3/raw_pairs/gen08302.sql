WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY sale_date DESC) AS rn FROM store_sales) SELECT id, store_id, sale_date FROM ranked WHERE rn <= 2 ORDER BY id
