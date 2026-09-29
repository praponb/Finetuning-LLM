with OrderSummary as (
    select order_id, round(stddev(unit_price), 2) as stddev_unit_price, round(avg(quantity), 2) as avg_quantity
    from order_items
    where quantity > 3
    group by order_id
)
select
    o.id,
    o.status,
    o.store_id,
    O.stddev_unit_price,
    O.avg_quantity
from orders o
left join OrderSummary O on o.id = O.order_id
where O.stddev_unit_price >= 40
order by stddev_unit_price desc;
