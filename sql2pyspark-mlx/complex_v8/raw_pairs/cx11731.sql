SELECT site_id AS ref_id, read_at, voltage
FROM meter_readings
WHERE quality IS NOT NULL
UNION ALL
SELECT id AS ref_id, read_at, voltage
FROM meter_readings
UNION ALL
SELECT site_id AS ref_id, read_at, voltage
FROM meter_readings
WHERE quality IN ('OK', 'EST')
  AND site_id <= 4
