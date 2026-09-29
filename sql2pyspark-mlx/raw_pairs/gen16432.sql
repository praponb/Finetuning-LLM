select
    id,
    order_date,
    channel,
    amount,
    discount,
    shipping_fee,
    lag(discount, 1) over (order by discount, id) as prev_discount,
    ntile(4) over (partition by store_id order by amount, id) as amount_bucket
from orders;
