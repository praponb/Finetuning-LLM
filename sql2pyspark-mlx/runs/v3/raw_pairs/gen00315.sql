SELECT cost AS value, 'shipment' AS source
FROM shipments
UNION ALL
SELECT unit_price AS value, 'order_item' AS source
FROM order_items
ORDER BY value
