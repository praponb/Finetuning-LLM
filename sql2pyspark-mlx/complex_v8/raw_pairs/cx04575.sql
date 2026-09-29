WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage >= 222.3
      AND kwh < 90.6
)
SELECT quality AS label
FROM prepared
WHERE read_at >= TIMESTAMP '2025-01-13 15:00:00'
  AND voltage <= 238.3
EXCEPT
SELECT tariff
FROM tariff_windows
WHERE tariff <> 'TOU'
ORDER BY label
