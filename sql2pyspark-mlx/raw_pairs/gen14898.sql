with order_totals as (
    select order_id, min(amount) as min_amount, max(amount) - min(amount) as amount_range
    from payments
    group by order_id
)
select
    o.customer_id,
    o.shipping_fee,
    ot.min_amount,
    ot.amount_range
from orders o
left join order_totals ot on o.id = ot.order_id
where ot.amount_range > 55
order by min_amount desc;
