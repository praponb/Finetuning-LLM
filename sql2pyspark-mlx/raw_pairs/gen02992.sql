SELECT id, status, paid_at, RANK() OVER (PARTITION BY status ORDER BY paid_at) AS paid_at_rank FROM payments WHERE order_id IN (4, 6, 12, 40) ORDER BY paid_at_rank
