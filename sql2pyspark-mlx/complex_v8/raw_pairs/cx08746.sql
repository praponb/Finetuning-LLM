WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE voltage >= 222.3
      AND read_at >= TIMESTAMP '2025-01-25 09:00:00'
)
SELECT id
FROM cleaned
WHERE quality_group IS NOT NULL
INTERSECT
SELECT site_id AS id
FROM meter_readings
WHERE read_at < TIMESTAMP '2025-02-24 10:00:00'
ORDER BY id
