select id, unit_price, quantity - unit_price as line_total
from order_items
where order_id in (4, 7, 29, 37)
order by unit_price, line_total, id
limit 10;
