WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(valid_from)) / 3600, 1) AS hours_valid_from_to_valid_to,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM tariff_windows
    WHERE valid_to >= TIMESTAMP '2025-02-15 04:00:00'
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(tariff_slug), ' ', '_')) AS tariff_slug_slug,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(valid_to)) / 3600, 1) AS hours_valid_to_to_valid_from
    FROM recent
    WHERE rate > 3.7
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        valid_to,
        rate,
        tariff_slug,
        hours_valid_to_to_valid_from,
        DATEDIFF(DATE '2025-06-30', valid_to) AS days_since_valid_to
    FROM enriched
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        valid_to,
        rate,
        tariff_slug,
        days_since_valid_to,
        DATE_TRUNC('HOUR', valid_to) AS valid_to_hour,
        DATE_FORMAT(valid_to, 'yyyy-MM-dd HH:00') AS valid_to_label
    FROM scored
)
-- Final projection
SELECT
    id,
    days_since_valid_to,
    valid_to_hour,
    valid_to_label
FROM labeled
ORDER BY valid_to_label NULLS LAST, valid_to_hour DESC
