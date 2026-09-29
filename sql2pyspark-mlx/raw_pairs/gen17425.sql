SELECT shipments.id, shipments.shipped_date, orders.discount, orders.shipping_fee, orders.status FROM shipments JOIN orders ON orders.id = shipments.order_id ORDER BY discount, orders.status;
