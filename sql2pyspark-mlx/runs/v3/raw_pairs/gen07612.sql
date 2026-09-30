select shipments.id, shipments.shipped_date from shipments where shipments.order_id in (select id from orders where store_id is null)
