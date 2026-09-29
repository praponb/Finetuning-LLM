SELECT shipped_date AS event_date FROM shipments INTERSECT SELECT opened_date AS event_date FROM stores WHERE city IN ('Singapore', 'London') ORDER BY event_date
