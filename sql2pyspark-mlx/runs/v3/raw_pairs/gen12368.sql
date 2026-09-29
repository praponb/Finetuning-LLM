SELECT delivered_date, 'shipment' AS source
FROM shipments
WHERE carrier IS NULL
UNION ALL
SELECT opened_date, 'store' AS source
FROM stores
WHERE region = 'North'
