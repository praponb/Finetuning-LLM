WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        kwh,
        voltage,
        ROUND(voltage - AVG(voltage) OVER w_site_id_read_at_month, 2) AS voltage_vs_site_id_read_at_month_avg,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id, MONTH(read_at))) AS distinct_quality_in_site_id_read_at_month,
        COUNT(*) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(kwh - AVG(kwh) OVER w_site_id_read_at_month, 2) AS kwh_vs_site_id_read_at_month_avg,
        LAST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id, MONTH(read_at)), 2) AS site_id_read_at_month_avg_voltage,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id, MONTH(read_at)), 0), 4) AS kwh_share_of_site_id_read_at_month,
        DENSE_RANK() OVER (ORDER BY voltage DESC) AS voltage_dense_rank,
        FIRST_VALUE(read_at) OVER (PARTITION BY YEAR(read_at) ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        LAG(kwh) OVER (ORDER BY voltage DESC, id) AS prev_kwh,
        COUNT(*) OVER (PARTITION BY YEAR(read_at) ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_2,
        LEAD(kwh, 3) OVER (ORDER BY voltage DESC, id) AS next3_kwh,
        DENSE_RANK() OVER w_read_at_year_by_read_at AS read_at_dense_rank,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh
    FROM base_readings
    WINDOW w_site_id_read_at_month AS (PARTITION BY site_id, MONTH(read_at)), w_read_at_year_by_read_at AS (PARTITION BY YEAR(read_at) ORDER BY read_at DESC, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(next3_kwh, read_at_dense_rank) AS greatest_next3_kwh_read_at_dense_rank,
        FLOOR(prev_kwh / 25) * 25 AS prev_kwh_bucket
    FROM metrics
),
rollup_base AS (
    -- Group-level totals
    SELECT
        read_at_dense_rank,
        PERCENTILE_APPROX(voltage_dense_rank, 0.5) AS median_voltage_dense_rank,
        MIN(distinct_quality_in_site_id_read_at_month) AS min_distinct_quality_in_site_id_read_at_month,
        MIN(first_read_at) AS first_first_read_at,
        MAX(first_read_at) AS last_first_read_at,
        MIN(last_known_read_at) AS first_last_known_read_at
    FROM tagged
    GROUP BY 1
    HAVING AVG(running_count_2) > 8
)
-- Final projection
SELECT
    read_at_dense_rank,
    median_voltage_dense_rank,
    min_distinct_quality_in_site_id_read_at_month,
    first_first_read_at,
    last_first_read_at,
    first_last_known_read_at,
    DATEDIFF(DATE '2026-01-01', first_last_known_read_at) AS days_since_first_last_known_read_at,
    DATE_FORMAT(first_last_known_read_at, 'yyyy-MM') AS first_last_known_read_at_label
FROM rollup_base
ORDER BY first_last_known_read_at DESC, read_at_dense_rank DESC NULLS FIRST
