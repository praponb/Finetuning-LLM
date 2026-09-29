select
    id,
    store_id,
    discount,
    order_date,
    channel,
    amount,
    dense_rank() over (partition by store_id order by discount) as discount_dense_rank,
    sum(amount) over (partition by year(order_date), channel order by order_date, id) as running_amount
from orders
