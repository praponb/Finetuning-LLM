SELECT id, site_id AS key_id, 'reading' AS source
FROM meter_readings
WHERE quality = 'EST'
UNION ALL
SELECT site_id, id, 'reading_2' AS source
FROM meter_readings
WHERE quality = 'OK'
