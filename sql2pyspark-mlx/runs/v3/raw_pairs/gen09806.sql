select product_id, min(unit_price) as min_unit_price
from order_items
group by 1
having min(unit_price) < 150
order by product_id;
