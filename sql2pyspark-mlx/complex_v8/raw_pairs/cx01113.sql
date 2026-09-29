WITH scoped_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE tariff NOT IN ('EV', 'FLAT')
      AND valid_to >= TIMESTAMP '2025-02-18 04:00:00'
)
SELECT
    id,
    rate,
    valid_to AS happened_at,
    'tariff' AS source
FROM scoped_tariffs
WHERE valid_from >= TIMESTAMP '2025-01-23 19:00:00'
  AND tariff IS NOT NULL
UNION ALL
SELECT
    id,
    voltage,
    read_at,
    'reading' AS source
FROM meter_readings
ORDER BY happened_at DESC, source
