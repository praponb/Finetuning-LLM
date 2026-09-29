with top_orders as (
    select
        id,
        channel,
        order_date,
        rank() over (partition by channel order by order_date) rn
    from orders
    where order_date between '2024-04-01' and '2024-07-01'
)
select id, channel, order_date
from top_orders
where rn <= 3
