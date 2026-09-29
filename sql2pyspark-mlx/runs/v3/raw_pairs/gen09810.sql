SELECT opened_date AS event_date FROM stores EXCEPT SELECT delivered_date AS event_date FROM shipments
