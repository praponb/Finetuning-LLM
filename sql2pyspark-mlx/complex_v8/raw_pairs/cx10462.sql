WITH filtered_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        valid_to + INTERVAL 2 HOURS AS valid_to_plus_2h
    FROM tariff_windows
    WHERE tariff IS NOT NULL
)
SELECT id, valid_from
FROM filtered_tariffs
WHERE valid_to_plus_2h >= TIMESTAMP '2025-01-17 09:00:00'
UNION ALL
SELECT site_id, read_at
FROM meter_readings
WHERE voltage <= 235.8
