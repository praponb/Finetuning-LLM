WITH base_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(valid_from)) / 60, 1) AS minutes_valid_from_to_valid_to,
        TO_DATE(valid_from) AS valid_from_date
    FROM tariff_windows
    WHERE (rate >= 3.0 OR tariff = 'EV')
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM base_tariffs
    WHERE minutes_valid_from_to_valid_to < (SELECT MAX(minutes_valid_from_to_valid_to) FROM base_tariffs)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LAST_DAY(valid_from_date) AS valid_from_date_month_end,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM filtered
)
SELECT
    id,
    tariff,
    valid_from,
    valid_to,
    rate,
    minutes_valid_from_to_valid_to,
    valid_from_date,
    valid_from_date_month_end,
    tariff_slug,
    ABS(rate - 5.5) AS rate_distance
FROM derived
ORDER BY minutes_valid_from_to_valid_to NULLS LAST, tariff DESC
