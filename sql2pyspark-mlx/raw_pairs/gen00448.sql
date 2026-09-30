select
    s.id,
    s.order_id,
    s.carrier,
    o.status as order_status,
    o.discount,
    o.order_date
from shipments s
left join orders o
    on s.order_id = o.id
