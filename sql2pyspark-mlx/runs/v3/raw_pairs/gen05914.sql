select order_id, unit_price, quantity
from order_items
where quantity between 4 and 7 or quantity between 3 and 5
order by quantity desc
