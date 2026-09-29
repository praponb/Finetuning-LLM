WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_to,
        rate,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM tariff_windows
    WHERE (MONTH(valid_from) IN (2, 3) OR valid_to >= TIMESTAMP '2025-02-18 04:00:00')
)
SELECT id, rate, valid_to AS event_time
FROM scoped
WHERE MONTH(valid_to) IN (1, 2)
UNION ALL
SELECT id, rate, valid_from AS event_time
FROM tariff_windows
UNION ALL
SELECT id, rate, valid_from AS event_time
FROM tariff_windows
ORDER BY id, event_time
