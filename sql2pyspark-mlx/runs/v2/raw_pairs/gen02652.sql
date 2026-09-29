select orders.status, shipments.order_id, shipments.delivered_date
from orders
left join shipments on shipments.order_id = orders.id
where shipments.order_id is not null;
