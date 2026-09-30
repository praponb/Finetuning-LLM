WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND read_at >= TIMESTAMP '2025-01-19 09:00:00'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_site_id, 0), 4) AS kwh_share_of_site_id,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY site_id, quality ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        RANK() OVER (PARTITION BY quality ORDER BY kwh) AS kwh_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY quality ORDER BY kwh, id), 3) AS kwh_cume_dist,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        DENSE_RANK() OVER (PARTITION BY site_id, quality ORDER BY voltage) AS voltage_dense_rank
    FROM base_readings
    WINDOW w_site_id AS (PARTITION BY site_id)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality,
    kwh_share_of_site_id,
    voltage_vs_site_id_avg,
    second_kwh,
    kwh_rank,
    kwh_cume_dist,
    distinct_quality_in_site_id,
    voltage_dense_rank,
    ABS(voltage_vs_site_id_avg - -98.0) AS voltage_vs_site_id_avg_distance
FROM with_windows
ORDER BY site_id DESC, kwh
