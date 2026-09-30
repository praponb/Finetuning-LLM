select
    shipments.id,
    shipments.shipped_date,
    shipments.carrier,
    shipments.cost
from shipments
where shipments.order_id in (select id from orders where channel in ('mobile', 'web'));
