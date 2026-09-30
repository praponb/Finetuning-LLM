select cost, 'shipment' as source
from shipments
union
select amount as cost, 'order' as source
from orders;
