select
    s.id,
    s.order_id,
    s.delivered_date,
    s.shipped_date
from shipments s
where s.order_id not in (select id from orders where shipping_fee <= 15);
