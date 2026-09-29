with recent_orders as (
    select *, datediff(current_date(), order_date) / 7 weeks_since_order_date
    from orders
    where channel in ('partner', 'store', 'mobile')
),
agg as (
    select
        channel,
        customer_id,
        sum(case when status = 'SHIPPED' then shipping_fee else 0 end) shipped_shipping_fee,
        min(shipping_fee) min_shipping_fee
    from recent_orders
    group by channel, customer_id
)
select
    channel,
    customer_id,
    shipped_shipping_fee,
    min_shipping_fee
from agg
where shipped_shipping_fee < 68
order by 3 desc, channel, customer_id
limit 5;
