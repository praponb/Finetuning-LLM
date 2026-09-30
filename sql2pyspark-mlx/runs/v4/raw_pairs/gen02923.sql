SELECT order_id, delivered_date
FROM shipments
WHERE carrier IS NULL
UNION
SELECT order_id, delivered_date
FROM shipments
WHERE carrier != 'DHL';
