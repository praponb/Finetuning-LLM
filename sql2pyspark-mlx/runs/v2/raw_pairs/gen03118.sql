select
    case when quantity >= 8 then 'high' when quantity >= 3 then 'mid' else 'low' end as quantity_bucket,
    product_id,
    round(stddev(quantity), 2) as stddev_quantity,
    sum(unit_price) as total_unit_price
from order_items
group by case when quantity >= 8 then 'high' when quantity >= 3 then 'mid' else 'low' end, product_id;
