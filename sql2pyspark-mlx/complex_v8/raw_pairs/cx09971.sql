SELECT quality, read_at AS happened_at
FROM meter_readings
WHERE kwh < 86.0
  AND read_at < TIMESTAMP '2025-03-08 10:00:00'
UNION
SELECT quality, read_at AS happened_at
FROM meter_readings
WHERE quality IN ('BAD', 'OK')
