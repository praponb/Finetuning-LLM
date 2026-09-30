select
    s.status,
    s.delivered_date,
    s.shipped_date,
    o.status as order_status,
    o.order_date
from shipments s
right join orders o on s.order_id = o.id
where o.amount <= 1080 and s.cost between 76 and 81
order by order_status;
