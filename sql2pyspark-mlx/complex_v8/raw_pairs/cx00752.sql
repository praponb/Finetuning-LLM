WITH filtered_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        UPPER(TRIM(tariff)) AS tariff_clean,
        DATE_TRUNC('HOUR', valid_from) AS valid_from_hour
    FROM tariff_windows
    WHERE HOUR(valid_to) BETWEEN 8 AND 10
),
latest_per_key AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        tariff_clean,
        valid_from_hour
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY valid_from_hour DESC, id DESC) AS row_num
        FROM filtered_tariffs
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    valid_from,
    valid_to,
    valid_from_hour,
    DATEDIFF(DATE '2025-06-30', valid_to) AS days_since_valid_to,
    LOWER(REGEXP_REPLACE(TRIM(tariff_clean), ' ', '_')) AS tariff_clean_slug
FROM latest_per_key
ORDER BY valid_from_hour, tariff_clean_slug DESC
