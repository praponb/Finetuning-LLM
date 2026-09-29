select orders.id, orders.channel from orders where orders.id in (select order_id from shipments)
