select
    o.id,
    o.order_date,
    o.amount,
    o.status
from orders o
where o.customer_id = 4 and not exists (select 1 from payments p where p.order_id = o.id and to_date(p.paid_at) >= '2023-05-01')
order by id desc;
