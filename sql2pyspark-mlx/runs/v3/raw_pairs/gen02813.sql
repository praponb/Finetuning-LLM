select product_id, min_quantity, unit_price_range
from (
    select product_id, min(quantity) as min_quantity, max(unit_price) - min(unit_price) as unit_price_range
    from order_items
    where product_id in (21, 31, 40)
    group by product_id
) as agg
where min_quantity < 3.1
order by product_id desc
