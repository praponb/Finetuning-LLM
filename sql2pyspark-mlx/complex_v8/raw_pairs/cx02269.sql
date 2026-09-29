WITH valid AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        COALESCE(rate, 0.0) AS rate_filled,
        DATE_FORMAT(valid_to, 'yyyy-MM-dd HH:00') AS valid_to_label,
        ABS(rate - 4.2) AS rate_distance
    FROM tariff_windows
    WHERE HOUR(valid_to) BETWEEN 7 AND 13
),
latest_per_key AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        rate_filled,
        valid_to_label,
        rate_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY rate_filled DESC, id DESC) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(rate_distance) OVER (PARTITION BY tariff) AS tariff_min_rate_distance,
        NTH_VALUE(rate_distance, 2) OVER (ORDER BY rate ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rate_distance,
        COUNT(*) OVER (PARTITION BY tariff) AS rows_in_tariff,
        LAST_VALUE(rate_distance) OVER (ORDER BY rate ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rate_distance
    FROM latest_per_key
)
-- Final projection
SELECT
    id,
    valid_to,
    rate_filled,
    valid_to_label,
    rate_distance,
    tariff_min_rate_distance,
    second_rate_distance,
    rows_in_tariff,
    last_rate_distance,
    COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
FROM metrics
ORDER BY tariff_min_rate_distance DESC NULLS FIRST, rate_filled DESC, id
LIMIT 20
