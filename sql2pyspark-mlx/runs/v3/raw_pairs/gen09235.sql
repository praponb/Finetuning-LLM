SELECT delivered_date
FROM shipments
WHERE status IN ('LOST', 'DELIVERED', 'IN_TRANSIT') AND cost BETWEEN 26 AND 84
