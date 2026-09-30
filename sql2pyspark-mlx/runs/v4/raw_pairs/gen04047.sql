with recent_orders as (
    select *
    from orders
    where channel in ('store', 'partner', 'web') or discount between 0.24 and 0.35
),
channel_summary as (
    select channel, sum(case when channel = 'mobile' then shipping_fee else 0 end) as mobile_shipping_fee
    from recent_orders
    group by channel
)
select channel, mobile_shipping_fee
from channel_summary
where mobile_shipping_fee > 60
order by mobile_shipping_fee desc, channel
limit 10
