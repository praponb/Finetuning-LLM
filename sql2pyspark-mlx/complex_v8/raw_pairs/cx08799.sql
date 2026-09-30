WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-31 10:00:00'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY MONTH(read_at), quality), 2) AS voltage_vs_read_at_month_quality_avg,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id ORDER BY read_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_2_prior,
        DENSE_RANK() OVER (PARTITION BY quality, site_id ORDER BY voltage) AS voltage_dense_rank,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY read_at DESC, id) AS read_at_dense_rank,
        LEAD(voltage) OVER (PARTITION BY site_id ORDER BY read_at DESC, id) AS next_voltage,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id), 2) AS site_id_avg_voltage,
        NTILE(3) OVER (PARTITION BY quality, site_id ORDER BY read_at, id) AS read_at_tercile,
        ROUND(AVG(voltage) OVER (PARTITION BY quality ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_3,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality ORDER BY read_at), 3) AS read_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY quality, site_id ORDER BY read_at), 3) AS read_at_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY read_at), 3) AS read_at_cume_dist_2
    FROM recent
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY quality ORDER BY read_at_cume_dist, id) AS rn,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id, read_at_dense_rank)) AS distinct_quality_in_site_id_read_at_dense_rank,
        next_voltage - LAG(next_voltage) OVER (PARTITION BY quality ORDER BY read_at_cume_dist, id) AS next_voltage_change,
        MIN(read_at_cume_dist) OVER w_site_id_read_at_dense_rank AS site_id_read_at_dense_rank_min_read_at_cume_dist
    FROM windowed
    WINDOW w_site_id_read_at_dense_rank AS (PARTITION BY site_id, read_at_dense_rank)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    voltage_avg_2_prior,
    read_at_dense_rank,
    next_voltage,
    read_at_tercile,
    voltage_avg_3,
    read_at_percent_rank,
    read_at_cume_dist,
    distinct_quality_in_site_id_read_at_dense_rank,
    ABS(read_at_percent_rank - 0.5) AS read_at_percent_rank_distance,
    ROUND(voltage * 1.07, 2) AS voltage_adj
FROM with_windows
WHERE voltage_dense_rank > 4
ORDER BY next_voltage DESC, id
LIMIT 5
