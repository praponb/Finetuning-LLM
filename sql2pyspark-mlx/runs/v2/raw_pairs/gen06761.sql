select orders.id, orders.status, count(shipments.id) as shipment_count
from orders
join shipments on shipments.order_id = orders.id
where shipments.status not in ('IN_TRANSIT', 'DELIVERED', 'LOST')
group by orders.id, orders.status
