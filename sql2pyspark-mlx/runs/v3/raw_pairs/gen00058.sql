select
    s.id,
    s.status,
    s.shipped_date,
    s.delivered_date
from shipments s
where s.order_id not in (select id from orders where customer_id is not null)
