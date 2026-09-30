WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        HOUR(read_at) AS read_at_hour,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM meter_readings
    WHERE (kwh BETWEEN 15.9 AND 85.7 OR site_id <= 2)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY read_at_hour)) AS distinct_quality_in_read_at_hour,
        MAX(voltage_per_kwh) OVER (PARTITION BY read_at_hour) AS read_at_hour_max_voltage_per_kwh,
        SUM(kwh) OVER w_read_at_hour AS read_at_hour_sum_kwh,
        RANK() OVER (PARTITION BY site_id ORDER BY read_at_hour ASC) AS read_at_hour_rank,
        LAST_VALUE(voltage_per_kwh) OVER (PARTITION BY site_id ORDER BY read_at_hour DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage_per_kwh,
        MAX(read_at_hour) OVER (PARTITION BY site_id ORDER BY read_at_hour ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS read_at_hour_max_6_prior,
        ROUND(CUME_DIST() OVER w_site_id_by_read_at_hour_ties, 3) AS read_at_hour_cume_dist,
        SUM(read_at_hour) OVER (PARTITION BY site_id ORDER BY read_at_hour DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS read_at_hour_sum_4
    FROM filtered
    WINDOW w_read_at_hour AS (PARTITION BY read_at_hour), w_site_id_by_read_at_hour_ties AS (PARTITION BY site_id ORDER BY read_at_hour ASC)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at_hour,
        voltage_per_kwh,
        distinct_quality_in_read_at_hour,
        read_at_hour_max_voltage_per_kwh,
        read_at_hour_sum_kwh,
        last_voltage_per_kwh,
        read_at_hour_max_6_prior,
        read_at_hour_cume_dist,
        read_at_hour_sum_4
    FROM ranked
    WHERE read_at_hour_rank = 1
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    read_at_hour,
    distinct_quality_in_read_at_hour,
    read_at_hour_max_voltage_per_kwh,
    read_at_hour_sum_kwh,
    last_voltage_per_kwh,
    read_at_hour_cume_dist,
    read_at_hour_sum_4,
    COALESCE(last_voltage_per_kwh, 0.0) AS last_voltage_per_kwh_filled
FROM first_rows
ORDER BY read_at_hour_cume_dist DESC, distinct_quality_in_read_at_hour
