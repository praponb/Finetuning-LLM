select
    shipments.id,
    shipments.carrier,
    shipments.shipped_date,
    orders.customer_id,
    stores.region
from shipments
left join orders on shipments.order_id = orders.id
full outer join stores on orders.store_id = stores.id
where orders.status in ('COMPLETED', 'RETURNED') or month(shipments.shipped_date) in (3, 7, 9)
