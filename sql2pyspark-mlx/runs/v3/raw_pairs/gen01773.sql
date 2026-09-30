select
    p.id,
    p.amount,
    o.customer_id,
    o.shipping_fee,
    o.channel
from payments p
inner join orders o on p.order_id = o.id
order by o.channel;
