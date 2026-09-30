WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 10 AND 13
      AND voltage >= 219.6
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        ROUND(AVG(days_since_read_at) OVER (PARTITION BY quality, site_id ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS days_since_read_at_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY voltage DESC NULLS FIRST, id) AS voltage_seq,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY days_since_read_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage,
        SUM(days_since_read_at) OVER (PARTITION BY quality, site_id ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_read_at,
        voltage - LAG(voltage) OVER (PARTITION BY quality, site_id ORDER BY voltage DESC NULLS FIRST, id) AS voltage_change,
        LAST_VALUE(days_since_read_at) OVER (PARTITION BY quality ORDER BY days_since_read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_read_at,
        MIN(kwh) OVER (PARTITION BY site_id, quality) AS site_id_quality_min_kwh,
        NTILE(4) OVER (PARTITION BY quality ORDER BY days_since_read_at, id) AS days_since_read_at_quartile,
        RANK() OVER (PARTITION BY quality ORDER BY days_since_read_at) AS days_since_read_at_rank,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY days_since_read_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(read_at), site_id ORDER BY days_since_read_at), 3) AS days_since_read_at_cume_dist,
        RANK() OVER w_read_at_year_site_id_by_days AS days_since_read_at_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY quality, site_id ORDER BY voltage DESC NULLS FIRST), 3) AS voltage_cume_dist,
        DENSE_RANK() OVER (ORDER BY days_since_read_at DESC NULLS FIRST) AS days_since_read_at_dense_rank
    FROM cleaned
    WINDOW w_read_at_year_site_id_by_days AS (PARTITION BY YEAR(read_at), site_id ORDER BY days_since_read_at)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(voltage_seq / NULLIF(last_days_since_read_at, 0), 3) AS voltage_seq_per_last_days_since_read_at
    FROM metrics
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY voltage_seq)) AS distinct_site_id_in_voltage_seq,
        MAX(voltage_cume_dist) OVER (PARTITION BY voltage_seq) AS voltage_seq_max_voltage_cume_dist,
        days_since_read_at_quartile - LAG(days_since_read_at_quartile) OVER (PARTITION BY site_id ORDER BY running_days_since_read_at, id) AS days_since_read_at_quartile_change,
        ROUND(AVG(minutes_since_prev_read_at) OVER (PARTITION BY voltage_seq), 2) AS voltage_seq_avg_minutes_since_prev_read_at
    FROM tagged
)
SELECT
    id,
    site_id,
    voltage,
    days_since_read_at_avg_3_prior,
    voltage_seq,
    first_known_voltage,
    running_days_since_read_at,
    site_id_quality_min_kwh,
    minutes_since_prev_read_at,
    days_since_read_at_cume_dist,
    days_since_read_at_rank_2,
    voltage_cume_dist,
    days_since_read_at_dense_rank,
    voltage_seq_per_last_days_since_read_at,
    distinct_site_id_in_voltage_seq,
    voltage_seq_max_voltage_cume_dist,
    days_since_read_at_quartile_change,
    voltage_seq_avg_minutes_since_prev_read_at
FROM windowed
ORDER BY id
