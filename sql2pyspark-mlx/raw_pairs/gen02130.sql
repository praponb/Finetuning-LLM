select o.id, o.order_date, o.channel, o.discount from orders o where not exists (select 1 from payments p where p.order_id = o.id) order by channel, id limit 50;
