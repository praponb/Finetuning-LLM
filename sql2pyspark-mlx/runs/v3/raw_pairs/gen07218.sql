select orders.status, count(distinct shipments.cost) unique_statuss
from shipments
left join orders on orders.id = shipments.order_id
join stores on orders.store_id = stores.id
where orders.store_id = 2
group by orders.status
order by orders.status
