select
    s.id,
    s.status,
    s.order_id,
    s.cost
from shipments s
where s.order_id in (select id from orders where discount between 0.11 and 0.14);
