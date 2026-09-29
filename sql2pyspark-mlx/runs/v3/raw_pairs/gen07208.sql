select quantity, unit_price, order_id
from order_items
where order_id in (24, 25)
order by unit_price desc
