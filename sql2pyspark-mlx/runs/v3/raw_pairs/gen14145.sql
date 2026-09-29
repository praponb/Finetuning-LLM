with order_totals as (
    select order_id, sum(case when carrier = 'DHL' then cost else 0 end) as dhl_cost
    from shipments
    where order_id = 34
    group by order_id
)
select
    o.id,
    o.shipping_fee,
    o.order_date,
    ot.dhl_cost
from orders o
join order_totals ot on o.id = ot.order_id
where ot.dhl_cost >= 29
