WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE (site_id >= 5 OR read_at >= TIMESTAMP '2025-01-19 12:00:00')
),
with_parent AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.quality_slug,
        si.tariff AS site_tariff
    FROM scoped s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        quality,
        site_tariff,
        LAG(voltage, 1, 0) OVER w_site_id_quality_by_voltage AS prev_voltage,
        MAX(voltage) OVER (ORDER BY voltage NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_max_3,
        RANK() OVER (ORDER BY voltage NULLS LAST, id) AS voltage_rank,
        LEAD(voltage) OVER (PARTITION BY site_id, quality ORDER BY voltage, id) AS next_voltage,
        MIN(voltage) OVER w_quality_site_tariff AS quality_site_tariff_min_voltage,
        SUM(voltage) OVER (ORDER BY voltage NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_site_id_site_tariff_by_volta)) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(CUME_DIST() OVER (PARTITION BY site_tariff ORDER BY voltage DESC NULLS FIRST, id), 3) AS voltage_cume_dist,
        NTILE(4) OVER (PARTITION BY site_tariff ORDER BY voltage DESC NULLS FIRST, id) AS voltage_quartile,
        ROUND(CUME_DIST() OVER w_site_id_site_tariff_by_volta_2, 3) AS voltage_cume_dist_2,
        LAST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY site_id, quality ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        MAX(voltage) OVER (ORDER BY voltage NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_tariff ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        LEAD(voltage, 3, 0) OVER (PARTITION BY site_id, site_tariff ORDER BY voltage DESC, id) AS next3_voltage,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg
    FROM with_parent
    WINDOW w_site_id_quality_by_voltage AS (PARTITION BY site_id, quality ORDER BY voltage, id), w_quality_site_tariff AS (PARTITION BY quality, site_tariff), w_site_id_site_tariff_by_volta AS (PARTITION BY site_id, site_tariff ORDER BY voltage DESC, id), w_site_id_site_tariff_by_volta_2 AS (PARTITION BY site_id, site_tariff ORDER BY voltage DESC)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        site_id,
        quality,
        site_tariff,
        prev_voltage,
        voltage_max_3,
        next_voltage,
        quality_site_tariff_min_voltage,
        running_voltage,
        minutes_since_prev_read_at,
        voltage_cume_dist,
        voltage_quartile,
        voltage_cume_dist_2,
        last_known_voltage,
        max_voltage_so_far,
        second_read_at,
        next3_voltage,
        voltage_vs_site_id_avg
    FROM with_windows
    WHERE voltage_rank = 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(minutes_since_prev_read_at) OVER w_quality AS quality_max_minutes_since_prev_read_at,
        NTH_VALUE(running_voltage, 2) OVER (ORDER BY max_voltage_so_far, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_voltage,
        LAG(next3_voltage) OVER (PARTITION BY site_tariff ORDER BY minutes_since_prev_read_at NULLS LAST, id) AS prev_next3_voltage
    FROM top_per_group
    WINDOW w_quality AS (PARTITION BY quality)
)
-- Final projection
SELECT
    id,
    site_id,
    quality,
    site_tariff,
    voltage_max_3,
    next_voltage,
    running_voltage,
    minutes_since_prev_read_at,
    voltage_cume_dist,
    voltage_quartile,
    voltage_cume_dist_2,
    max_voltage_so_far,
    second_read_at,
    voltage_vs_site_id_avg,
    quality_max_minutes_since_prev_read_at,
    second_running_voltage,
    prev_next3_voltage
FROM metrics
ORDER BY voltage_quartile DESC
