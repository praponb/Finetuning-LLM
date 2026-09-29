select o.id, o.amount, o.store_id from orders o where not exists (select 1 from payments p where p.order_id = o.id) order by store_id, id limit 50;
