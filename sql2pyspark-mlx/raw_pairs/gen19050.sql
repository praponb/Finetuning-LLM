select
    shipments.shipped_date,
    shipments.status,
    orders.channel,
    orders.order_date,
    stores.region,
    stores.sqft
from shipments
left join orders on orders.id = shipments.order_id
full outer join stores on stores.id = orders.store_id
where stores.region = 'West'
