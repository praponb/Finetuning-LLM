select o.store_id, max(s.cost) max_cost, count(distinct s.cost) unique_carriers
from shipments s
full outer join orders o on s.order_id = o.id
where s.status in ('IN_TRANSIT', 'LOST')
group by o.store_id
order by o.store_id, unique_carriers desc;
