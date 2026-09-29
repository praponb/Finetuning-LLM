SELECT shipped_date, 'shipment' record_type
FROM shipments
WHERE shipped_date IS NOT NULL
UNION ALL
SELECT opened_date, 'store' record_type
FROM stores
WHERE opened_date IS NOT NULL
ORDER BY record_type
