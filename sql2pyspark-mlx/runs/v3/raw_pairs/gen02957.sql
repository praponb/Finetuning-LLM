WITH numbered AS (SELECT id, status, paid_at, order_id, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) row_num FROM payments) SELECT * FROM numbered WHERE row_num = 1;
