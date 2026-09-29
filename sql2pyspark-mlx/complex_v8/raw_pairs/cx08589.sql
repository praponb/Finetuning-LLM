WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
),
rollup_base AS (
    -- Summarise per group
    SELECT
        YEAR(read_at) AS read_at_year,
        ROUND(100 * COUNT(CASE WHEN quality = 'EST' THEN 1 END) / COUNT(*), 1) AS pct_quality_est,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM recent_readings
    GROUP BY YEAR(read_at)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE pct_quality_est > (SELECT AVG(pct_quality_est) FROM rollup_base)
)
-- Final projection
SELECT
    read_at_year,
    pct_quality_est,
    COALESCE(pct_quality_est, 0.0) AS pct_quality_est_filled,
    COALESCE(pct_quality_est, 0.0) AS pct_quality_est_filled_2
FROM screened
WHERE pct_quality_est >= 40.0
ORDER BY pct_quality_est, pct_quality_est_filled DESC NULLS FIRST
