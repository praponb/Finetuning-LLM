select o.channel, o.status, max(s.delivered_date) as last_delivered_date
from shipments s
inner join orders o on s.order_id = o.id
where o.order_date between '2023-11-01' and '2025-01-27'
group by o.channel, o.status
