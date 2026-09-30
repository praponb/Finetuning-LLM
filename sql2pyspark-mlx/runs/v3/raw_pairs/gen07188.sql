select shipments.id, shipments.delivered_date, shipments.shipped_date, shipments.cost, orders.discount from shipments full outer join orders on shipments.order_id = orders.id order by discount desc;
