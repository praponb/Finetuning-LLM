select status, order_id from payments intersect all select status, order_id from payments where paid_at < '2023-09-01'
