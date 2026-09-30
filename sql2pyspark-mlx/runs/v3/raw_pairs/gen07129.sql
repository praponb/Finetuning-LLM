select order_id, count(unit_price) as unit_price_count
from order_items
where unit_price <= 310
group by order_id
having unit_price_count >= 4;
