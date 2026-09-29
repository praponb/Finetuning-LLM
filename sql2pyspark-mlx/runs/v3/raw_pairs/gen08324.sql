SELECT shipped_date, 'shipment' AS source
FROM shipments
UNION ALL
SELECT order_date AS shipped_date, 'order' AS source
FROM orders
