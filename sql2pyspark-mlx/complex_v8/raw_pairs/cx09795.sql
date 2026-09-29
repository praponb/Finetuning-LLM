WITH scoped_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        LOWER(TRIM(tariff)) AS tariff_clean
    FROM tariff_windows
    WHERE tariff = 'EV'
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        tariff_clean,
        ROUND(rate * 1.1, 2) AS rate_adj
    FROM scoped_tariffs
)
-- Final projection
SELECT id, tariff, valid_from
FROM derived
ORDER BY tariff, id NULLS LAST
LIMIT 10
