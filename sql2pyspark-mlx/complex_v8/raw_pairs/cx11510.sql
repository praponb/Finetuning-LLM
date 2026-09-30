SELECT zone
FROM sites
WHERE zone = 'Z4'
EXCEPT ALL
SELECT quality
FROM meter_readings
WHERE TO_DATE(read_at) < DATE '2025-03-08'
  AND quality IN ('BAD', 'OK')
ORDER BY zone
