WITH valid_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
)
SELECT 'tariff' AS origin, tariff AS name, id AS ref_id
FROM valid_tariffs
WHERE tariff = 'TOU'
  AND valid_to >= TIMESTAMP '2025-02-16 05:00:00'
UNION
SELECT 'reading' AS origin, quality, id
FROM meter_readings
