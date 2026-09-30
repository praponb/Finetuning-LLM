WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage - kwh AS voltage_minus_kwh
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 3)
      AND kwh <= 106.0
)
SELECT site_id AS qty, read_at
FROM base_readings
UNION ALL
SELECT id, valid_to
FROM tariff_windows
WHERE valid_to < TIMESTAMP '2025-03-27 13:00:00'
  AND rate < 6.1
