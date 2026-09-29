with order_stats as (
    select order_id, count(*) as cnt
    from order_items
    where quantity between 4 and 6
    group by order_id
)
select orders.store_id, orders.shipping_fee, order_stats.cnt
from orders
inner join order_stats on orders.id = order_stats.order_id
where order_stats.cnt >= 8
order by shipping_fee asc, order_stats.cnt asc
