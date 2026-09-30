select id, unit_price, row_number() over (order by unit_price, id) as unit_price_row
from order_items
order by unit_price desc
