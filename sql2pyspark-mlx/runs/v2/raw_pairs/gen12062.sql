select
    case when quantity >= 9 then 'high' when quantity >= 5 then 'mid' else 'low' end quantity_bucket,
    product_id,
    round(avg(unit_price), 2) avg_unit_price,
    max(quantity) max_quantity,
    max(unit_price) max_unit_price
from order_items
group by 1, 2;
