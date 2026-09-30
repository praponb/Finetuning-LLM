SELECT opened_date, 'account' AS source FROM accounts WHERE customer_id IN (9, 36) UNION SELECT delivered_date, 'shipment' AS source FROM shipments WHERE status <> 'DELIVERED'
