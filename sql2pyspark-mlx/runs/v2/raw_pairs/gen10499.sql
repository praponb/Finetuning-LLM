select id, order_id, paid_at, amount, lag(amount, 1) over (partition by order_id order by paid_at, id) as prev_amount from payments where method in ('bank_transfer', 'cash', 'card');
