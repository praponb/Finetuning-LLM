SELECT id, method, paid_at, ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at, id) AS row_num FROM payments;
