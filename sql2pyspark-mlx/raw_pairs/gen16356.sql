select o.status, round(avg(s.cost), 2) as avg_cost, max(s.cost) - min(s.cost) as cost_range
from shipments s
left join orders o on s.order_id = o.id
group by o.status
having max(s.cost) - min(s.cost) >= 41
order by avg_cost
