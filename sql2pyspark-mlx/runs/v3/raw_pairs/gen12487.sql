SELECT shipped_date, 'shipment' AS source
FROM shipments
UNION ALL
SELECT opened_date, 'store' AS source
FROM stores;
