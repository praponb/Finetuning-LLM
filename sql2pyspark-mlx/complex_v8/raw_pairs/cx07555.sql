WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 110.5 THEN 'HIGH' WHEN kwh >= 37.1 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-25 14:00:00'
      AND kwh >= 37.1
)
SELECT id AS key_id, read_at AS happened_at, site_id
FROM scoped
WHERE kwh_tier NOT IN ('LOW', 'HIGH')
  AND quality <> 'EST'
UNION
SELECT id, read_at, site_id
FROM meter_readings
ORDER BY key_id DESC, site_id
