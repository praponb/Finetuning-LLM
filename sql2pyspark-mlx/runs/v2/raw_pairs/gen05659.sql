select product_id, stddev_unit_price
from (
    select product_id, round(stddev(unit_price), 2) as stddev_unit_price
    from order_items
    group by product_id
) as t
where stddev_unit_price >= 21
