SELECT id, amount, status, paid_at, ROW_NUMBER() OVER (ORDER BY amount, id) AS row_num, RANK() OVER (PARTITION BY status ORDER BY paid_at) AS paid_at_rank FROM payments WHERE amount < 1280
