select product_id, stddev_quantity
from (
    select product_id, round(stddev(quantity), 2) stddev_quantity
    from inventory
    group by product_id
) s
where stddev_quantity > 46
order by product_id desc, 2 desc
limit 5
