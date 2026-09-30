SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS row_num FROM payments) t WHERE row_num = 1;
