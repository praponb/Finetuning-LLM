SELECT status, 'order' AS source FROM orders UNION ALL SELECT status, 'shipment' AS source FROM shipments WHERE delivered_date BETWEEN '2023-04-01' AND '2024-04-01'
