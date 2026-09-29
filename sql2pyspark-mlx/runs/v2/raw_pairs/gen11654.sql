WITH top_payments AS (SELECT id, method, amount, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount, id) AS row_num FROM payments) SELECT id, method, amount FROM top_payments WHERE row_num <= 2;
