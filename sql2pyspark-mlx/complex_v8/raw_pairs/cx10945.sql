SELECT id, quality
FROM meter_readings
INTERSECT ALL
SELECT id, tariff
FROM sites
WHERE installed_kw < 284.0
ORDER BY quality, id
