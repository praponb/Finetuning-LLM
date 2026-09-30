select
    id,
    channel,
    status,
    round(shipping_fee, 0) as shipping_fee_rounded
from orders
where year(order_date) >= 2023;
