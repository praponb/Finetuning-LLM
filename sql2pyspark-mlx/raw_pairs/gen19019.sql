select o.id, o.channel, count(p.id) as payment_count
from orders o
left join payments p on o.id = p.order_id
group by o.id, o.channel
having count(p.id) > 1
order by 3 asc, channel asc;
