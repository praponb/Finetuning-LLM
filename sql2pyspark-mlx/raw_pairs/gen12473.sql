select
    order_id,
    case when unit_price >= 430 then 'high' when unit_price >= 70 then 'mid' else 'low' end as unit_price_bucket,
    sum(quantity * unit_price) as revenue,
    count(distinct unit_price) as unique_orders,
    count(distinct unit_price) as unique_products
from order_items
where product_id in (7, 8, 26, 38)
group by 1, 2
