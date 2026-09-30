SELECT site_id AS ref_id, quality AS label
FROM meter_readings
WHERE quality IS NOT NULL
EXCEPT
SELECT id, quality
FROM meter_readings
WHERE voltage BETWEEN 215.1 AND 232.4
ORDER BY ref_id
