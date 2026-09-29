SELECT order_id, delivered_date, CASE WHEN cost >= 100 THEN 'High' WHEN cost >= 35 THEN 'Medium' ELSE 'Low' END cost_level
FROM shipments
WHERE status != 'DELIVERED' AND cost BETWEEN 37 AND 44
