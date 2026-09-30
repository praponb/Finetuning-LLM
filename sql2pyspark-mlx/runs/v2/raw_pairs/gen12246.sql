select order_id from payments where status in ('SUCCESS', 'FAILED', 'REFUNDED') intersect select order_id from payments where paid_at < '2023-03-01'
