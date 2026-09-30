select
    s.id,
    s.carrier,
    s.shipped_date,
    s.status
from shipments s
where s.carrier = 'UPS' and s.order_id in (select id from orders where discount >= 0.36)
