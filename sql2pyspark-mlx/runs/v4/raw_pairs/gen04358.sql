SELECT status, 'order' AS source FROM orders UNION SELECT status, 'shipment' AS source FROM shipments WHERE MONTH(delivered_date) IN (1, 11, 12)
