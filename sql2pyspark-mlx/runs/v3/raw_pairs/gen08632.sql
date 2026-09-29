select *
from (
    select product_id, round(avg(quantity), 2) as avg_quantity
    from order_items
    where product_id is not null
    group by product_id
) as t
where avg_quantity > 21;
