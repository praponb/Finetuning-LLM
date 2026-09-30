WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh < 89.8
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality ORDER BY voltage DESC, id), 3) AS voltage_cume_dist,
        MAX(kwh) OVER (PARTITION BY site_id) AS site_id_max_kwh,
        NTILE(10) OVER (PARTITION BY site_id, quality ORDER BY voltage DESC, id) AS voltage_decile,
        NTILE(10) OVER (PARTITION BY quality ORDER BY voltage ASC NULLS LAST, id) AS voltage_decile_2,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id), 2) AS kwh_vs_site_id_avg,
        SUM(voltage) OVER (PARTITION BY site_id) AS site_id_sum_voltage,
        COUNT(*) OVER w_quality_site_id AS rows_in_quality_site_id,
        RANK() OVER (PARTITION BY quality ORDER BY kwh DESC) AS kwh_rank,
        ROUND(voltage - AVG(voltage) OVER w_site_id, 2) AS voltage_vs_site_id_avg,
        FIRST_VALUE(kwh) OVER (PARTITION BY site_id, quality ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh
    FROM scoped_readings
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id), w_site_id AS (PARTITION BY site_id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        voltage_decile_2,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        COUNT(*) FILTER (WHERE quality = 'EST') AS n_quality_est,
        MAX(voltage_vs_site_id_avg) AS max_voltage_vs_site_id_avg
    FROM with_windows
    WHERE site_id_sum_voltage <= 1442.0
    GROUP BY 1, 2, 3
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY n_quality_est ASC NULLS LAST) AS n_quality_est_dense_rank,
        ROUND(AVG(n_quality_est) OVER (PARTITION BY quality, n_quality_est ORDER BY max_voltage_vs_site_id_avg DESC, voltage_decile_2, quality, read_at_ym ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS n_quality_est_avg_7
    FROM stats
)
SELECT
    voltage_decile_2,
    quality,
    read_at_ym,
    n_quality_est,
    max_voltage_vs_site_id_avg,
    n_quality_est_dense_rank,
    ROUND(n_quality_est_avg_7 * 0.9, 2) AS n_quality_est_avg_7_adj
FROM windowed
