select
    o.id,
    o.store_id,
    o.channel,
    o.order_date
from orders o
where not exists (select 1 from shipments s where s.order_id = o.id)
order by o.id desc, order_date desc;
