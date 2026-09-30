select p.id, p.method, p.paid_at
from payments p
where p.order_id in (12, 23, 29) and p.order_id in (select id from orders where amount <= 1560)
order by p.paid_at desc;
