select shipments.id, shipments.carrier from shipments where shipments.order_id in (select id from orders where store_id in (4, 12, 27, 39));
