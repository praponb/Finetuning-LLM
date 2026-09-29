select
    s.id,
    s.order_id,
    o.channel,
    o.discount,
    o.customer_id
from shipments s
full outer join orders o on s.order_id = o.id;
