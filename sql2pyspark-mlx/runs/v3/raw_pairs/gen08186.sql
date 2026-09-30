select product_id, quantity
from order_items
where unit_price < 210
intersect
select product_id, quantity
from order_items
where order_id in (12, 17, 20, 25);
