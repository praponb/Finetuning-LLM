select
    product_id,
    sum(unit_price) as total_unit_price,
    count(*) as cnt,
    dense_rank() over (order by sum(unit_price) desc) as rnk
from order_items
group by product_id
order by total_unit_price desc;
