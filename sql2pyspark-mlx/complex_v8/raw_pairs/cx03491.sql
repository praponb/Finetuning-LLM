SELECT id, read_at AS happened_at, kwh
FROM meter_readings
WHERE voltage >= 218.5
UNION ALL
SELECT id, read_at, kwh
FROM meter_readings
WHERE quality = 'OK'
  AND site_id IS NOT NULL
UNION ALL
SELECT site_id, read_at, voltage
FROM meter_readings
ORDER BY happened_at DESC
