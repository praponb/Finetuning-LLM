select o.id, o.customer_id from orders o where exists (select 1 from payments p where p.order_id = o.id and p.status = 'FAILED') order by customer_id, id desc
