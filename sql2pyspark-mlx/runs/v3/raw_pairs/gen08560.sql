select order_id from payments where amount < 800 union all select order_id from payments where paid_at < '2024-05-01';
