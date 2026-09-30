SELECT last_restock, 'inventory' AS source FROM inventory UNION ALL SELECT delivered_date AS last_restock, 'shipment' AS source FROM shipments
