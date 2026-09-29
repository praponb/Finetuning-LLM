select
    order_id,
    case when quantity >= 7 then 'high' when quantity >= 3 then 'mid' else 'low' end as quantity_bucket,
    case when unit_price >= 450 then 'high' when unit_price >= 170 then 'mid' else 'low' end as unit_price_bucket,
    sum(unit_price) as total_unit_price
from order_items
where order_id in (9, 16, 23, 40) or product_id is null
group by order_id, quantity_bucket, unit_price_bucket
having sum(unit_price) >= 2100
