select *
from (
    select order_id, count(distinct quantity) unique_products
    from order_items
    group by order_id
) x
where unique_products > 7
order by order_id
limit 3;
