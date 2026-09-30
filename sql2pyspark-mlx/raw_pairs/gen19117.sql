SELECT 'shipment' AS record_type, shipped_date FROM shipments UNION SELECT 'customer' AS record_type, signup_date FROM customers WHERE segment IN ('Corporate', 'SMB')
