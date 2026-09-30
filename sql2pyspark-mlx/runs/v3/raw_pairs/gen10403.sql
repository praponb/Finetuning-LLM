select o.id, o.store_id, o.amount from orders o where o.id in (select order_id from shipments where carrier = 'FedEx');
