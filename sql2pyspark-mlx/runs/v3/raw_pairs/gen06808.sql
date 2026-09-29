select o.channel, round(avg(s.cost), 2) as avg_cost, max(s.delivered_date) as latest_delivered_date
from shipments s
left outer join orders o on s.order_id = o.id
where s.shipped_date > '2024-03-21'
group by o.channel;
