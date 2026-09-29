WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug,
        LENGTH(TRIM(tariff)) AS tariff_len
    FROM tariff_windows
    WHERE tariff NOT IN ('TOU', 'EV')
),
deduped AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        tariff_slug,
        tariff_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY valid_from DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM deduped
    WHERE tariff_len < (SELECT MAX(tariff_len) FROM deduped)
)
-- Final projection
SELECT
    id,
    valid_from,
    valid_to,
    tariff_len
FROM with_activity
ORDER BY valid_to, tariff_len, id
LIMIT 10
