WITH base AS (
    -- Filter tariff_windows
    SELECT *
    FROM tariff_windows
    WHERE (valid_to >= TIMESTAMP '2025-02-16 05:00:00' OR rate > 4.2)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        ABS(rate - 4.2) AS rate_distance,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM base
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        rate - rate_distance AS rate_minus_rate_distance,
        COALESCE(rate_distance, 0.0) AS rate_distance_filled,
        ABS(rate_distance - 5.2) AS rate_distance_distance
    FROM annotated
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        valid_from,
        tariff_ref,
        rate_minus_rate_distance,
        rate_distance_filled,
        rate_distance_distance,
        SUM(rate_minus_rate_distance) OVER (PARTITION BY TO_DATE(valid_to)) AS valid_to_to_date_sum_rate_minus_rate_distance,
        FIRST_VALUE(valid_from) IGNORE NULLS OVER (ORDER BY rate_distance_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_valid_from
    FROM tagged
)
SELECT
    id,
    valid_from,
    tariff_ref,
    rate_minus_rate_distance,
    rate_distance_filled,
    rate_distance_distance,
    valid_to_to_date_sum_rate_minus_rate_distance,
    first_known_valid_from
FROM metrics
ORDER BY tariff_ref DESC NULLS FIRST, id
LIMIT 10
