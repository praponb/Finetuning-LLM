select
    o.channel,
    o.status,
    count(s.cost) as cost_count,
    round(avg(s.cost), 2) as avg_cost
from shipments s
right join orders o on s.order_id = o.id
inner join customers c on o.customer_id = c.id
group by o.channel, o.status
having cost_count >= 5;
