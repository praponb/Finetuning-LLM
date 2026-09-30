SELECT shipped_date AS event_date FROM shipments WHERE status = 'IN_TRANSIT' UNION ALL SELECT last_restock AS event_date FROM inventory WHERE quantity IS NULL
