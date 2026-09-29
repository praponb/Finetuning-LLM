select unit_price
from order_items
union
select amount as unit_price
from transactions
order by unit_price
