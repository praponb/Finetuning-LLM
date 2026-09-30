select o.channel, avg(s.cost) as avg_cost
from shipments s
inner join orders o on s.order_id = o.id
where o.order_date <= '2024-09-01'
group by o.channel;
