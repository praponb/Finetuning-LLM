select c.country, sum(s.cost) as total_cost
from shipments s
full outer join orders o on s.order_id = o.id
join customers c on o.customer_id = c.id
group by c.country
having total_cost >= 410
order by total_cost desc
