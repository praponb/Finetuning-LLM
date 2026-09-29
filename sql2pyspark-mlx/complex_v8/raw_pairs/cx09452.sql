WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE kwh >= 20.4
      AND voltage <= 233.6
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM base_readings
    WHERE kwh < (SELECT MAX(kwh) FROM base_readings)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        quality,
        YEAR(read_at) AS read_at_year,
        MAX(kwh) AS max_kwh,
        COUNT(*) FILTER (WHERE quality IN ('BAD', 'EST')) AS n_quality_in_bad_est,
        COUNT(*) AS n_rows
    FROM filtered
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(n_quality_in_bad_est - 4) AS n_quality_in_bad_est_distance,
        ROUND(max_kwh / NULLIF(n_quality_in_bad_est, 0), 3) AS max_kwh_per_n_quality_in_bad_est,
        ROUND(max_kwh * 0.9, 2) AS max_kwh_adj
    FROM rollup_base
)
-- Native SQL UNPIVOT (wide to long)
SELECT quality, read_at_year, field, metric_value
FROM (
    SELECT quality, read_at_year, max_kwh, max_kwh_adj, max_kwh_per_n_quality_in_bad_est
    FROM tagged
) src
UNPIVOT (
    metric_value FOR field IN (max_kwh, max_kwh_adj, max_kwh_per_n_quality_in_bad_est)
) unp
