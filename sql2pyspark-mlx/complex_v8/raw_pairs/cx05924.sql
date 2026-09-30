WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 8 AND 14
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        quality,
        ROUND(SUM(kwh), 2) AS total_kwh,
        COUNT(*) AS n_rows,
        SUM(kwh) FILTER (WHERE quality = 'BAD') AS kwh_quality_bad,
        ROUND(AVG(kwh), 2) AS avg_kwh,
        MAX(read_at) AS last_read_at,
        SUM(kwh) AS total_kwh_2
    FROM base
    WHERE site_id >= 5
    GROUP BY quality
    HAVING COUNT(*) >= 2
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY n_rows ORDER BY last_read_at DESC), 3) AS last_read_at_percent_rank,
        ROUND(total_kwh / NULLIF(SUM(total_kwh) OVER (PARTITION BY quality, n_rows), 0), 4) AS total_kwh_share_of_quality_n_rows,
        LAST_VALUE(total_kwh) OVER (PARTITION BY n_rows ORDER BY last_read_at DESC, quality ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_total_kwh,
        SUM(n_rows) OVER (PARTITION BY quality, n_rows) AS quality_n_rows_sum_n_rows
    FROM totals
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(quality, '#', CAST(n_rows AS STRING)) AS quality_ref,
        CASE WHEN total_kwh_2 >= 576.0 THEN 'A' WHEN total_kwh_2 >= 252.0 THEN 'B' ELSE 'C' END AS total_kwh_2_tier,
        ABS(kwh_quality_bad - 360.0) AS kwh_quality_bad_distance
    FROM ranked
    WHERE last_read_at_percent_rank >= 0.1
)
-- Subtotals with CUBE
SELECT
    total_kwh_2_tier,
    n_rows,
    quality,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT total_kwh_2_tier) AS distinct_total_kwh_2_tiers,
    COUNT(*) AS cnt,
    ROUND(SUM(last_read_at_percent_rank), 2) AS total_last_read_at_percent_rank
FROM annotated
GROUP BY CUBE(total_kwh_2_tier, n_rows, quality)
ORDER BY total_kwh_2_tier NULLS LAST, n_rows NULLS LAST, quality NULLS LAST
