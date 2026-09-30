WITH base_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref
    FROM tariff_windows
    WHERE rate >= 2.2
      AND valid_from >= TIMESTAMP '2025-01-24 14:00:00'
)
SELECT id AS key_id
FROM base_tariffs
INTERSECT ALL
SELECT id AS key_id
FROM meter_readings
ORDER BY key_id
