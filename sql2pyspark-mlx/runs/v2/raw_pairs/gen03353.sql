select orders.id, orders.order_date, orders.amount from orders where exists (select 1 from shipments where shipments.order_id = orders.id);
