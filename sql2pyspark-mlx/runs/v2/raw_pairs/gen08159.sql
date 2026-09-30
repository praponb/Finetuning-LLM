with order_stats as (
    select order_id, avg(amount) avg_amount, max(paid_at) last_paid_at
    from payments
    where order_id is not null
    group by order_id
)
select
    o.id,
    o.discount,
    os.avg_amount,
    os.last_paid_at
from orders o
join order_stats os on o.id = os.order_id
where os.avg_amount >= 1090
