select
    id,
    channel,
    order_date,
    discount,
    lag(discount, 1) over (partition by channel order by order_date, id) as prev_discount
from orders
where channel in ('store', 'mobile')
order by discount desc, channel
