select o.id, o.store_id, o.amount from orders o where o.status = 'PENDING' and not exists (select 1 from shipments s where s.order_id = o.id) order by store_id desc, id limit 1
