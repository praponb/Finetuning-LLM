select orders.status, count(*) total_count
from shipments
left join orders on orders.id = shipments.order_id
join stores on orders.store_id = stores.id
group by orders.status
having count(*) >= 3
