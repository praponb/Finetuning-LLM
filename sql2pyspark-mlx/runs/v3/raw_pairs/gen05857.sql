select o.channel, o.status, sum(p.amount) as total_amount
from payments p
left join orders o on p.order_id = o.id
where to_date(p.paid_at) < '2023-11-24'
group by o.channel, o.status
order by o.channel
