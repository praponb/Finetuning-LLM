select orders.id, orders.shipping_fee, orders.customer_id, orders.channel from orders where orders.id in (select order_id from shipments where order_id = 5) order by channel desc;
