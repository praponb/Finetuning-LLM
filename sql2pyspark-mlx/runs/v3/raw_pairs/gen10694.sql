select
    id,
    channel,
    amount,
    customer_id,
    discount,
    min(amount) over (partition by channel) channel_min_amount,
    row_number() over (partition by customer_id order by discount desc, id) discount_row
from orders
order by id desc
