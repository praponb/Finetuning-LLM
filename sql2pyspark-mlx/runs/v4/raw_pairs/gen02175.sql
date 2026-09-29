with OrderItemsByProduct as (
    select product_id, avg(quantity) avg_quantity, round(stddev(quantity), 2) stddev_quantity
    from order_items
    group by product_id
)
select
    p.id,
    p.price,
    O.avg_quantity,
    O.stddev_quantity
from products p
left outer join OrderItemsByProduct O on p.id = O.product_id
order by stddev_quantity desc;
