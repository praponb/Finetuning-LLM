WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM meter_readings
    WHERE quality IN ('EST', 'BAD')
      AND kwh > 33.9
)
SELECT quality, voltage, 'reading' AS src
FROM filtered
UNION ALL
SELECT tariff, rate, 'tariff' AS src
FROM tariff_windows
WHERE rate >= 2.2
  AND tariff = 'FLAT'
