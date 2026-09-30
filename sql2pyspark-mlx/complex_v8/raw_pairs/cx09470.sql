SELECT quality
FROM meter_readings
WHERE quality IN ('EST', 'BAD')
INTERSECT
SELECT tariff
FROM sites
ORDER BY quality DESC
