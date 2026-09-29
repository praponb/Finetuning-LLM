WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ABS(kwh - 63.6) AS kwh_distance
    FROM meter_readings
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        kwh_distance,
        DENSE_RANK() OVER (PARTITION BY quality, MONTH(read_at) ORDER BY voltage, id) AS voltage_dense_rank,
        ROUND(AVG(voltage) OVER (ORDER BY read_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_1_prior,
        DENSE_RANK() OVER w_all_by_read_at_ties AS read_at_dense_rank,
        LAG(kwh, 3, 0) OVER (PARTITION BY MONTH(read_at), quality ORDER BY read_at, id) AS prev3_kwh,
        ROUND(voltage - AVG(voltage) OVER w_quality_site_id, 2) AS voltage_vs_quality_site_id_avg,
        SUM(voltage) OVER w_quality_site_id AS quality_site_id_sum_voltage,
        NTILE(3) OVER (ORDER BY voltage NULLS LAST, id) AS voltage_tercile,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, site_id ORDER BY voltage ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_6_prior
    FROM recent_readings
    WINDOW w_all_by_read_at_ties AS (ORDER BY read_at DESC), w_quality_site_id AS (PARTITION BY quality, site_id)
),
leaders AS (
    -- Top 5 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        kwh_distance,
        voltage_avg_1_prior,
        read_at_dense_rank,
        prev3_kwh,
        voltage_vs_quality_site_id_avg,
        quality_site_id_sum_voltage,
        voltage_tercile,
        voltage_avg_6_prior
    FROM with_windows
    WHERE voltage_dense_rank <= 5
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    kwh_distance,
    voltage_avg_1_prior,
    read_at_dense_rank,
    prev3_kwh,
    voltage_vs_quality_site_id_avg,
    voltage_tercile,
    voltage_avg_6_prior,
    HOUR(read_at) AS read_at_hour,
    read_at + INTERVAL 15 MINUTES AS read_at_plus_15m
FROM leaders
ORDER BY site_id DESC, read_at_plus_15m
