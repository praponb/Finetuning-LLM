select
    product_id,
    case when quantity >= 7 then 'high' when quantity >= 3 then 'mid' else 'low' end as quantity_bucket,
    sum(quantity * unit_price) as revenue,
    count(*) as order_item_count,
    round(stddev(quantity), 2) as stddev_quantity
from order_items
where unit_price > 320
group by product_id, case when quantity >= 7 then 'high' when quantity >= 3 then 'mid' else 'low' end
