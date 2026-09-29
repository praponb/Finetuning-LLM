select
    s.id,
    s.order_id,
    s.shipped_date,
    s.cost,
    o.order_date
from shipments s
full outer join orders o on o.id = s.order_id
where o.order_date >= '2025-10-01'
order by order_id desc;
