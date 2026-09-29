SELECT status, 'order' AS source FROM orders WHERE shipping_fee BETWEEN 18 AND 27 UNION SELECT status, 'shipment' AS source FROM shipments WHERE delivered_date < '2024-07-24'
