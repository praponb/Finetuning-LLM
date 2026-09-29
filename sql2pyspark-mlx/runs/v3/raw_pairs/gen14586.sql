select product_id, count(*) as total_count
from order_items
where product_id is not null
group by product_id
order by product_id
