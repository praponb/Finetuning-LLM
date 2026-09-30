SELECT site_id, id AS key_id, 'reading' AS origin
FROM meter_readings
WHERE site_id IS NOT NULL
  AND kwh >= 52.5
UNION ALL
SELECT site_id, id AS key_id, 'reading_2' AS origin
FROM meter_readings
WHERE quality IS NOT NULL
  AND HOUR(read_at) BETWEEN 9 AND 14
ORDER BY origin, site_id
