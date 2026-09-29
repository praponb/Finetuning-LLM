SELECT id AS key_id, tariff
FROM sites
UNION ALL
SELECT site_id AS key_id, quality AS tariff
FROM meter_readings
