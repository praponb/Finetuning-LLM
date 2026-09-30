select * from (select order_id, method, amount from payments where paid_at < '2024-09-01')
pivot (avg(amount) for method in ('paypal', 'card'))
order by order_id
