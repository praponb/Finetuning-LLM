WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(TRIM(quality)) AS quality_clean,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-02-18 10:00:00'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY voltage, id) AS seq_no,
        DENSE_RANK() OVER (ORDER BY kwh) AS kwh_dense_rank,
        ROUND(kwh_per_voltage / NULLIF(SUM(kwh_per_voltage) OVER (PARTITION BY quality), 0), 4) AS kwh_per_voltage_share_of_quality,
        NTILE(5) OVER w_all_by_kwh AS kwh_quintile,
        MIN(voltage) OVER (PARTITION BY quality) AS quality_min_voltage,
        LAG(read_at) OVER (ORDER BY kwh ASC, id) AS prev_read_at,
        COUNT(*) OVER (ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, site_id ORDER BY voltage, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_1_prior,
        RANK() OVER (ORDER BY kwh ASC) AS kwh_rank,
        NTH_VALUE(kwh_per_voltage, 2) OVER (ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh_per_voltage,
        RANK() OVER (ORDER BY kwh, id) AS kwh_rank_2,
        COUNT(*) OVER w_quality AS rows_in_quality
    FROM prepared
    WINDOW w_all_by_kwh AS (ORDER BY kwh ASC, id), w_quality AS (PARTITION BY quality)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality_clean,
        kwh_per_voltage,
        seq_no,
        kwh_dense_rank,
        kwh_per_voltage_share_of_quality,
        kwh_quintile,
        quality_min_voltage,
        prev_read_at,
        running_count,
        first_known_read_at,
        voltage_avg_1_prior,
        kwh_rank,
        second_kwh_per_voltage,
        kwh_rank_2,
        rows_in_quality,
        ROUND(kwh / NULLIF(running_count, 0), 3) AS kwh_per_running_count
    FROM ranked
    WHERE kwh_per_voltage > 4.0
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(kwh_quintile) OVER (PARTITION BY seq_no ORDER BY kwh_per_voltage_share_of_quality ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_quintile_so_far,
        LEAD(prev_read_at, 3) OVER w_kwh_rank_kwh_rank_2_by_volta AS next3_prev_read_at,
        ROUND(kwh_rank - AVG(kwh_rank) OVER w_prev_read_at_month, 2) AS kwh_rank_vs_prev_read_at_month_avg,
        ROUND(quality_min_voltage - AVG(quality_min_voltage) OVER w_prev_read_at_month, 2) AS quality_min_voltage_vs_prev_read_at_month_avg,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_read_at)) AS rows_in_prev_read_at_month,
        ROUND(AVG(kwh_rank_2) OVER (ORDER BY seq_no, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS kwh_rank_2_avg_2_prior,
        ROUND(CUME_DIST() OVER w_all_by_seq_no_ties, 3) AS seq_no_cume_dist,
        LAG(kwh_rank_2) OVER (PARTITION BY seq_no ORDER BY kwh_per_voltage_share_of_quality ASC, id) AS prev_kwh_rank_2
    FROM tagged
    WINDOW w_kwh_rank_kwh_rank_2_by_volta AS (PARTITION BY kwh_rank, kwh_rank_2 ORDER BY voltage DESC, id), w_prev_read_at_month AS (PARTITION BY DATE_TRUNC('MONTH', prev_read_at)), w_all_by_seq_no_ties AS (ORDER BY seq_no)
)
SELECT
    id,
    read_at,
    voltage,
    kwh_per_voltage,
    seq_no,
    kwh_per_voltage_share_of_quality,
    kwh_quintile,
    quality_min_voltage,
    running_count,
    first_known_read_at,
    voltage_avg_1_prior,
    kwh_rank,
    second_kwh_per_voltage,
    kwh_rank_2,
    rows_in_quality,
    min_kwh_quintile_so_far,
    next3_prev_read_at,
    kwh_rank_vs_prev_read_at_month_avg,
    quality_min_voltage_vs_prev_read_at_month_avg,
    rows_in_prev_read_at_month,
    kwh_rank_2_avg_2_prior,
    SUBSTRING(TRIM(quality_clean), 1, 3) AS quality_clean_prefix,
    FLOOR(quality_min_voltage / 500) * 500 AS quality_min_voltage_bucket
FROM with_windows
ORDER BY kwh_per_voltage, id
LIMIT 5
