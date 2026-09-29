with numbered as (
    select
        id,
        customer_id,
        order_date,
        channel,
        dense_rank() over (partition by customer_id order by order_date desc) as rnk
    from orders
)
select
    id,
    customer_id,
    order_date,
    channel
from numbered
where rnk <= 5
