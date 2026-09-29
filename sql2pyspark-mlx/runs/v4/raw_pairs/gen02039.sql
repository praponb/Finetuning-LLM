select
    o.id,
    o.status,
    o.customer_id,
    o.amount
from orders o
where not exists (select 1 from payments p where p.order_id = o.id and p.amount between 990 and 1090);
