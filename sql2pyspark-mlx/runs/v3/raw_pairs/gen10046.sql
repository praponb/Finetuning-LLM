select amount
from payments
intersect
select unit_price
from order_items
