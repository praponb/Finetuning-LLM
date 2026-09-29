WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LEAST(voltage, kwh) AS least_voltage_kwh,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
    WHERE quality NOT IN ('EST', 'OK')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(voltage) OVER w_quality_site_id AS quality_site_id_min_voltage,
        ROUND(CUME_DIST() OVER w_all_by_read_at_year_ties, 3) AS read_at_year_cume_dist,
        COUNT(*) OVER (ORDER BY least_voltage_kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_read_at_year_site_id_by_read)) / 60, 1) AS minutes_since_prev_read_at,
        RANK() OVER (PARTITION BY read_at_year, site_id ORDER BY read_at DESC) AS read_at_rank
    FROM filtered
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id), w_all_by_read_at_year_ties AS (ORDER BY read_at_year DESC), w_read_at_year_site_id_by_read AS (PARTITION BY read_at_year, site_id ORDER BY read_at DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE read_at_rank = 1
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        voltage,
        quality,
        read_at_year,
        quality_site_id_min_voltage,
        read_at_year_cume_dist,
        running_count,
        minutes_since_prev_read_at,
        read_at_rank,
        DENSE_RANK() OVER (PARTITION BY read_at_rank ORDER BY read_at) AS read_at_dense_rank,
        ROUND(AVG(read_at_year) OVER (PARTITION BY read_at_rank ORDER BY read_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS read_at_year_avg_6_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality ORDER BY kwh ASC, id), 3) AS kwh_percent_rank
    FROM top_rows
)
SELECT DISTINCT quality_site_id_min_voltage
FROM with_windows
ORDER BY quality_site_id_min_voltage DESC NULLS FIRST
