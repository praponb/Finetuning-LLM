select *, quantity - unit_price as line_total, quantity * unit_price as line_total_2
from order_items
where order_id = 10
order by id asc
