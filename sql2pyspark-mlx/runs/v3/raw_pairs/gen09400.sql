select
    store_id,
    status,
    shipping_fee,
    date_add(order_date, 65) as order_date_plus_65d,
    length(channel) as channel_length
from orders
where discount between 0.13 and 0.31;
