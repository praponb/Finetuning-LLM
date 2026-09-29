SELECT order_date AS event_date, 'order' AS source FROM orders UNION SELECT shipped_date AS event_date, 'shipment' AS source FROM shipments
