SELECT 'shipment' AS origin, delivered_date
FROM shipments
WHERE delivered_date BETWEEN '2023-10-07' AND '2024-12-26'
UNION
SELECT 'order' AS origin, order_date
FROM orders
ORDER BY delivered_date DESC
