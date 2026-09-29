select max(unit_price) - min(unit_price) as unit_price_range
from order_items
order by unit_price_range desc
