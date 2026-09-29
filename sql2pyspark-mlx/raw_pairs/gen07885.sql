select 'order_item' as record_type, unit_price
from order_items
union
select 'shipment' as record_type, cost
from shipments
order by record_type
