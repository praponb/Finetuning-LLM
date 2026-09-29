SELECT shipped_date, 'shipment' AS source
FROM shipments
WHERE cost BETWEEN 30 AND 43
UNION
SELECT order_date AS shipped_date, 'order' AS source
FROM orders
ORDER BY source DESC;
