with valid_orders as (
    select *, initcap(trim(channel)) channel_title
    from orders
    where status = 'PENDING' and month(order_date) in (7, 10, 12)
),
OrdersStats as (
    select channel, sort_array(collect_set(status)) status_list
    from valid_orders
    group by channel
)
select *
from OrdersStats;
