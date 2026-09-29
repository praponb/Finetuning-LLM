select
    p.id,
    p.status,
    p.method,
    o.customer_id,
    o.channel
from payments p
left join orders o on p.order_id = o.id
order by channel;
