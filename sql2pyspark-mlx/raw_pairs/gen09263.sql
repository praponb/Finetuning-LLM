SELECT 'store' AS record_type, store_name
FROM stores
UNION
SELECT 'shipment' AS record_type, status
FROM shipments
