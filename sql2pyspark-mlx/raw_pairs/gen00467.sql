select
    p.status,
    o.amount,
    o.discount,
    o.channel
from payments p
right join orders o on p.order_id = o.id
where p.method not in ('paypal', 'cash', 'card');
