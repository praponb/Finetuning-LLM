SELECT quality
FROM meter_readings
WHERE quality NOT IN ('EST', 'OK')
EXCEPT ALL
SELECT zone
FROM sites
ORDER BY quality DESC
