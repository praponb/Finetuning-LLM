SELECT site_id, read_at AS event_time
FROM meter_readings
UNION ALL
SELECT site_id, read_at AS event_time
FROM meter_readings
ORDER BY event_time DESC, site_id
