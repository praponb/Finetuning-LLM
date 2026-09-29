WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage <= 229.1
      AND read_at >= TIMESTAMP '2025-01-19 13:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ROUND(PERCENT_RANK() OVER (ORDER BY read_at DESC, id), 3) AS read_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, quality ORDER BY voltage), 3) AS voltage_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality ORDER BY read_at DESC, id), 3) AS read_at_cume_dist,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality), 2) AS kwh_vs_quality_avg,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY YEAR(read_at)), 0), 4) AS kwh_share_of_read_at_year,
        MAX(voltage) OVER (ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS voltage_max_7,
        LEAD(read_at) OVER (ORDER BY kwh DESC NULLS FIRST, id) AS next_read_at,
        ROUND(AVG(voltage) OVER (PARTITION BY YEAR(read_at)), 2) AS read_at_year_avg_voltage,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality ORDER BY read_at DESC), 3) AS read_at_cume_dist_2
    FROM valid
),
derived AS (
    -- Label and score rows
    SELECT *, CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM metrics
),
summary AS (
    -- Group-level totals
    SELECT
        quality,
        TO_DATE(read_at) AS read_at_day,
        DATE_FORMAT(next_read_at, 'yyyy-MM') AS next_read_at_ym,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        COUNT(*) FILTER (WHERE quality IS NOT NULL) AS n_quality_is_not_null
    FROM derived
    GROUP BY 1, 2, 3
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(n_quality_is_not_null) OVER w_quality AS quality_max_n_quality_is_not_null,
        ROUND(AVG(n_quality_is_not_null) OVER (PARTITION BY quality, DATE_TRUNC('MONTH', read_at_day) ORDER BY n_quality_is_not_null, quality, read_at_day, next_read_at_ym ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS n_quality_is_not_null_avg_6_prior
    FROM summary
    WINDOW w_quality AS (PARTITION BY quality)
)
SELECT
    quality,
    read_at_day,
    n_quality_is_not_null,
    quality_max_n_quality_is_not_null,
    COALESCE(n_quality_is_not_null, 0) AS n_quality_is_not_null_filled
FROM with_windows
