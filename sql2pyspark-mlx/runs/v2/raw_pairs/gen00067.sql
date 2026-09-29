select p.id, p.paid_at, p.method, p.amount from payments p where p.order_id in (select id from orders where amount between 950 and 1210) order by p.id desc
