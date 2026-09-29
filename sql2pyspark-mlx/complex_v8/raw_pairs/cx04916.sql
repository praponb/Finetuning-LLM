WITH base_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tariff_windows
    WHERE MONTH(valid_from) IN (1, 3)
)
SELECT id, valid_from AS happened_at
FROM base_tariffs
UNION ALL
SELECT id, valid_from
FROM tariff_windows
ORDER BY id, happened_at
