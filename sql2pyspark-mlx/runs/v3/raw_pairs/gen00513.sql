select amount, order_id from payments where order_id is not null and status in ('SUCCESS', 'REFUNDED', 'FAILED') order by amount desc
