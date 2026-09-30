select
    s.id,
    s.order_id,
    s.shipped_date,
    s.cost,
    o.status,
    o.order_date,
    o.store_id
from shipments s
left outer join orders o on s.order_id = o.id
where o.channel in ('mobile', 'web', 'partner') and s.order_id is not null
order by s.shipped_date, store_id desc
