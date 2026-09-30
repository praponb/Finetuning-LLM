with order_totals as (
    select order_id, sum(case when status = 'SUCCESS' then 1 else 0 end) as success_count, min(paid_at) as min_paid_at
    from payments
    group by order_id
)
select
    o.customer_id,
    o.amount,
    ot.success_count,
    ot.min_paid_at
from orders o
inner join order_totals ot on o.id = ot.order_id
where ot.success_count >= 58
order by success_count asc, min_paid_at asc
