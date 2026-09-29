WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('YEAR', read_at) AS read_at_year
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 10:00:00'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        read_at_year,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id, quality), 2) AS voltage_vs_site_id_quality_avg,
        LAG(read_at_year) OVER (ORDER BY voltage DESC NULLS FIRST, id) AS prev_read_at_year,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY YEAR(read_at)), 2) AS kwh_vs_read_at_year_avg,
        DENSE_RANK() OVER (PARTITION BY quality, site_id ORDER BY voltage NULLS LAST) AS voltage_dense_rank,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_quality_read_at_year_by_read)) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_read_at_year, 0), 4) AS kwh_share_of_read_at_year,
        ROUND(PERCENT_RANK() OVER (ORDER BY voltage DESC NULLS FIRST), 3) AS voltage_percent_rank,
        LAG(read_at, 3) OVER (PARTITION BY quality, YEAR(read_at) ORDER BY read_at DESC, id) AS prev3_read_at,
        SUM(voltage) OVER (PARTITION BY quality, site_id ORDER BY voltage NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_sum_2_prior,
        MAX(voltage) OVER (PARTITION BY quality, YEAR(read_at) ORDER BY read_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS voltage_max_2,
        MAX(voltage) OVER w_read_at_year AS read_at_year_max_voltage,
        LEAD(read_at, 2) OVER (PARTITION BY quality, site_id ORDER BY voltage NULLS LAST, id) AS next2_read_at
    FROM valid_readings
    WINDOW w_quality_read_at_year_by_read AS (PARTITION BY quality, YEAR(read_at) ORDER BY read_at DESC, id), w_read_at_year AS (PARTITION BY YEAR(read_at))
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        read_at_year,
        voltage_vs_site_id_quality_avg,
        prev_read_at_year,
        kwh_vs_read_at_year_avg,
        minutes_since_prev_read_at,
        kwh_share_of_read_at_year,
        voltage_percent_rank,
        prev3_read_at,
        voltage_sum_2_prior,
        voltage_max_2,
        read_at_year_max_voltage,
        next2_read_at
    FROM with_windows
    WHERE voltage_dense_rank <= 3
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        voltage_vs_site_id_quality_avg,
        prev_read_at_year,
        kwh_vs_read_at_year_avg,
        kwh_share_of_read_at_year,
        voltage_percent_rank,
        prev3_read_at,
        read_at_year_max_voltage,
        LAST_VALUE(kwh_vs_read_at_year_avg) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', prev_read_at_year) ORDER BY kwh_vs_read_at_year_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh_vs_read_at_year_avg,
        ROUND(read_at_year_max_voltage / NULLIF(SUM(read_at_year_max_voltage) OVER w_read_at_year_year_next2_read, 0), 4) AS read_at_year_max_voltage_share_of_read_at_year_y,
        COUNT(*) OVER w_read_at_year_year_next2_read AS rows_in_read_at_year_year_next2_read_at_month
    FROM leaders
    WINDOW w_read_at_year_year_next2_read AS (PARTITION BY YEAR(read_at_year), DATE_TRUNC('MONTH', next2_read_at))
)
SELECT
    id,
    read_at,
    kwh,
    voltage,
    prev_read_at_year,
    kwh_vs_read_at_year_avg,
    kwh_share_of_read_at_year,
    read_at_year_max_voltage,
    last_known_kwh_vs_read_at_year_avg,
    rows_in_read_at_year_year_next2_read_at_month
FROM with_trends
ORDER BY rows_in_read_at_year_year_next2_read_at_month DESC NULLS FIRST
