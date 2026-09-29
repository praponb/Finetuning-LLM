select unit_price
from order_items
union all
select cost as unit_price
from shipments
