select o.channel, count(*) as cnt
from payments p
full outer join orders o on p.order_id = o.id
where o.channel is not null
group by o.channel;
