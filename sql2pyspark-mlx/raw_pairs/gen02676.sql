select product_id, unit_price, order_id
from order_items
where (unit_price >= 180 and order_id in (5, 40)) or product_id in (25, 26, 32)
order by product_id desc;
