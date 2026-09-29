with order_totals as (
    select order_id, max(cost) - min(cost) as cost_range
    from shipments
    where delivered_date >= '2024-10-01'
    group by order_id
)
select
    o.id,
    o.customer_id,
    o.channel,
    ot.cost_range
from orders o
join order_totals ot on o.id = ot.order_id
where ot.cost_range > 36
order by id
limit 10;
