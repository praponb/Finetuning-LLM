select
    case when quantity >= 8 then 'high' when quantity >= 3 then 'mid' else 'low' end as quantity_bucket,
    order_id,
    sum(unit_price * quantity) as revenue,
    count(quantity) as quantity_count
from order_items
group by quantity_bucket, order_id
having sum(unit_price * quantity) < 47;
