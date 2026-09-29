select o.id, o.store_id from orders o where o.status = 'SHIPPED' and o.id in (select order_id from shipments);
