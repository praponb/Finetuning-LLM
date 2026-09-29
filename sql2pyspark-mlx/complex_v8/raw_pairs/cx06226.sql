WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh <= 106.0
      AND voltage <= 237.5
),
joined AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        si.zone AS site_zone,
        si.site_name
    FROM prepared s
    LEFT JOIN sites si ON si.id = s.site_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        quality,
        site_zone,
        site_name,
        MAX(voltage) OVER (PARTITION BY quality) AS quality_max_voltage,
        MIN(voltage) OVER (PARTITION BY quality) AS quality_min_voltage,
        MIN(voltage) OVER (PARTITION BY site_zone ORDER BY read_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far,
        SUM(voltage) OVER (PARTITION BY quality, site_zone) AS quality_site_zone_sum_voltage,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, site_zone)) AS distinct_site_id_in_quality_site_zone,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_zone, quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at_2,
        RANK() OVER (PARTITION BY site_zone ORDER BY read_at ASC NULLS LAST) AS read_at_rank,
        MIN(voltage) OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far_2,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, site_zone), 2) AS quality_site_zone_avg_voltage,
        LAG(voltage) OVER w_site_zone_quality_by_read_at_2 AS prev_voltage,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(read_at) ORDER BY read_at), 3) AS read_at_cume_dist
    FROM joined
    WINDOW w_site_zone_quality_by_read_at_2 AS (PARTITION BY site_zone, quality ORDER BY read_at, id)
)
-- Final projection
SELECT
    id,
    quality,
    site_zone,
    site_name,
    quality_max_voltage,
    quality_min_voltage,
    quality_site_zone_sum_voltage,
    distinct_site_id_in_quality_site_zone,
    first_known_read_at_2,
    read_at_rank,
    min_voltage_so_far_2
FROM sequenced
ORDER BY distinct_site_id_in_quality_site_zone, id DESC
