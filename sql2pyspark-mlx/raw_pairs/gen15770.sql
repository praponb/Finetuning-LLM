select o.id, o.order_date
from orders o
where o.store_id in (6, 8, 26) and exists (select 1 from payments p where p.order_id = o.id and p.status = 'REFUNDED')
order by id;
