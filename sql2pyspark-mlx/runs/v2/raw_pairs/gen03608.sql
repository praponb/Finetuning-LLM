WITH top_payments AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS row_num FROM payments) SELECT * FROM top_payments WHERE row_num <= 3
