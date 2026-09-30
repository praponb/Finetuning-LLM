WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 9 AND 15
      AND quality NOT IN ('EST', 'BAD')
)
SELECT id, quality
FROM scoped
WHERE HOUR(read_at) BETWEEN 10 AND 12
  AND quality NOT IN ('OK', 'EST')
UNION
SELECT id, quality
FROM meter_readings
ORDER BY quality, id
