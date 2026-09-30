select id, paid_at, amount, order_id from payments where method is null and status in ('REFUNDED', 'SUCCESS', 'FAILED');
