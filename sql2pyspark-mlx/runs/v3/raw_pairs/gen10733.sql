SELECT id, order_id, shipped_date, CONCAT_WS('-', carrier, status) AS carrier_status_key FROM shipments WHERE delivered_date BETWEEN '2023-03-06' AND '2025-04-01' AND order_id = 10
