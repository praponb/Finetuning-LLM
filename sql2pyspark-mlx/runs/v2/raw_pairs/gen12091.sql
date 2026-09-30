SELECT carrier, cost, order_id, CONCAT_WS('-', status, carrier) AS status_carrier_key FROM shipments ORDER BY order_id DESC, 2 DESC
