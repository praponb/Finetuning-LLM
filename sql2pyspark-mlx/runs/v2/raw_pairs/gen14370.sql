select order_id, max(quantity) - min(quantity) as quantity_range
from order_items
group by order_id
having max(quantity) - min(quantity) >= 50
order by quantity_range desc
