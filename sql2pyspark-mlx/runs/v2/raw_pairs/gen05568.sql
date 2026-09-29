WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount DESC, id) AS rn FROM orders) SELECT * FROM ranked WHERE rn <= 5 ORDER BY status
