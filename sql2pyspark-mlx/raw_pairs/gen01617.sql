select o.id, o.status, o.discount, o.channel from orders o where exists (select 1 from payments p where p.order_id = o.id and hour(p.paid_at) between 8 and 14)
