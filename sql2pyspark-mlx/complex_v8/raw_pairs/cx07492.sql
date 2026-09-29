WITH base_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM tariff_windows
    WHERE tariff = 'FLAT'
)
SELECT
    tariff_or_unknown,
    valid_from AS happened_at,
    rate,
    'tariff' AS source
FROM base_tariffs
UNION
SELECT
    tariff AS tariff_or_unknown,
    valid_from AS happened_at,
    rate,
    'tariff_2' AS source
FROM tariff_windows
ORDER BY happened_at
