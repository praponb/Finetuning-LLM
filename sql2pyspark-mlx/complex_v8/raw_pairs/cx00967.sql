WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
)
SELECT id, site_id AS qty
FROM valid
UNION ALL
SELECT site_id AS id, id AS qty
FROM meter_readings
WHERE kwh >= 26.9
  AND site_id <= 6
ORDER BY id DESC
