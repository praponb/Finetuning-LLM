WITH filtered_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM tariff_windows
    WHERE HOUR(valid_from) BETWEEN 10 AND 12
      AND rate < 7.6
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        valid_to,
        rate,
        tariff_or_unknown,
        tariff_slug,
        NTH_VALUE(rate, 2) OVER (PARTITION BY tariff ORDER BY valid_from DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rate,
        MIN(rate) OVER (PARTITION BY tariff_or_unknown) AS tariff_or_unknown_min_rate,
        COUNT(*) OVER (PARTITION BY tariff_or_unknown) AS rows_in_tariff_or_unknown,
        ROUND(CUME_DIST() OVER (PARTITION BY tariff ORDER BY valid_from DESC, id), 3) AS valid_from_cume_dist
    FROM filtered_tariffs
),
latest_per_key AS (
    -- Keep the earliest row per tariff_or_unknown
    SELECT
        id,
        valid_to,
        rate,
        tariff_or_unknown,
        tariff_slug,
        second_rate,
        tariff_or_unknown_min_rate,
        rows_in_tariff_or_unknown,
        valid_from_cume_dist
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff_or_unknown ORDER BY valid_to, id) AS recency_rank
        FROM sequenced
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    rate,
    tariff_or_unknown,
    tariff_slug,
    rows_in_tariff_or_unknown,
    valid_from_cume_dist,
    TO_DATE(valid_to) AS valid_to_date,
    DATE_FORMAT(valid_to, 'yyyy-MM-dd HH:00') AS valid_to_label
FROM latest_per_key
WHERE tariff_slug IS NOT NULL
ORDER BY rows_in_tariff_or_unknown DESC, valid_from_cume_dist DESC
