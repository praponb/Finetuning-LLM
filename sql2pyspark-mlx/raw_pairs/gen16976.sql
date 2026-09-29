with OrderSummary as (
    select order_id, round((100 * sum(case when status = 'SUCCESS' then 1 else 0 end)) / count(*), 1) pct_success
    from payments
    group by order_id
)
select
    o.id,
    o.discount,
    o.shipping_fee,
    O.pct_success
from orders o
inner join OrderSummary O on o.id = O.order_id
where O.pct_success > 49
order by o.shipping_fee asc
