select
    id,
    channel,
    amount,
    shipping_fee,
    datediff(current_date(), order_date) / 30 as months_since_order_date
from orders
where channel in ('store', 'mobile')
