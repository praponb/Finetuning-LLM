SELECT delivered_date AS event_date FROM shipments WHERE delivered_date IS NOT NULL UNION SELECT last_restock AS event_date FROM inventory WHERE last_restock IS NOT NULL ORDER BY event_date
