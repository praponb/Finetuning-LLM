select product_id, sum(unit_price) as total_unit_price
from order_items
group by product_id
order by product_id
