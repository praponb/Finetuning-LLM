select
    order_id,
    sum(quantity) as total_quantity,
    count(*) as cnt,
    rank() over (order by sum(quantity) desc) as rnk
from order_items
group by order_id;
