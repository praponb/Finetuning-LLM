select paid_at from payments where status not in ('FAILED', 'SUCCESS', 'REFUNDED') and order_id is not null and paid_at < '2024-08-14' order by paid_at;
