select o.id, o.discount, o.customer_id from orders o where not exists (select 1 from payments p where p.order_id = o.id) order by customer_id desc
