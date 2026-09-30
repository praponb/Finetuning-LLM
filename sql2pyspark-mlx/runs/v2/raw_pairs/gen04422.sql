select o.status, round(stddev(s.cost), 2) as stddev_cost
from shipments s
inner join orders o on o.id = s.order_id
where o.store_id is null
group by o.status
having round(stddev(s.cost), 2) > 78
order by status desc, stddev_cost desc
