select o.status, sum(case when s.carrier = 'UPS' then s.cost else 0 end) as ups_cost
from shipments s
inner join orders o on s.order_id = o.id
group by o.status
having ups_cost > 74
order by o.status desc
limit 10;
