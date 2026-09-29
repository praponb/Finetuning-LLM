SELECT id AS entity_id, tariff AS label
FROM sites
INTERSECT ALL
SELECT site_id, quality
FROM meter_readings
ORDER BY label, entity_id
