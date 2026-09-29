SELECT id, method, amount, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS row_num FROM payments ORDER BY 4 DESC;
