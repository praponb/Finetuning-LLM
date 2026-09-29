select
    s.id,
    s.cost,
    o.shipping_fee,
    o.channel
from shipments s
left outer join orders o on s.order_id = o.id;
