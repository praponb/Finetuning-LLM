WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ABS(kwh - 53.6) AS kwh_distance,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
    WHERE kwh >= 45.3
      AND quality NOT IN ('OK', 'EST')
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.kwh_distance,
        s.voltage_filled,
        si.zone AS site_zone
    FROM scoped s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, site_zone)) AS distinct_site_id_in_quality_site_zone,
        RANK() OVER w_site_zone_by_voltage_filled AS voltage_filled_rank,
        LAST_VALUE(kwh_distance) OVER (PARTITION BY site_zone ORDER BY voltage_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh_distance,
        ROUND(kwh - AVG(kwh) OVER w_quality_site_zone, 2) AS kwh_vs_quality_site_zone_avg,
        MAX(voltage_filled) OVER (PARTITION BY site_zone ORDER BY voltage_filled, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_filled_max_2_prior,
        RANK() OVER w_site_zone_quality_by_kwh_tie AS kwh_rank,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, site_zone), 2) AS quality_site_zone_avg_voltage,
        DENSE_RANK() OVER (PARTITION BY site_zone ORDER BY voltage_filled, id) AS voltage_filled_dense_rank
    FROM joined
    WINDOW w_site_zone_by_voltage_filled AS (PARTITION BY site_zone ORDER BY voltage_filled), w_quality_site_zone AS (PARTITION BY quality, site_zone), w_site_zone_quality_by_kwh_tie AS (PARTITION BY site_zone, quality ORDER BY kwh DESC NULLS FIRST)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY distinct_site_id_in_quality_site_zone ORDER BY quality_site_zone_avg_voltage, id) AS quality_site_zone_avg_voltage_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY kwh_rank ORDER BY kwh DESC NULLS FIRST, id), 3) AS kwh_cume_dist,
        MAX(voltage_filled_rank) OVER (PARTITION BY site_zone) AS site_zone_max_voltage_filled_rank,
        RANK() OVER (PARTITION BY kwh_rank ORDER BY kwh DESC NULLS FIRST) AS kwh_rank_2,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY site_zone)) AS distinct_site_id_in_site_zone,
        DENSE_RANK() OVER (PARTITION BY kwh_rank ORDER BY kwh DESC NULLS FIRST, id) AS kwh_dense_rank
    FROM with_trends
)
SELECT
    id,
    voltage,
    quality,
    kwh_distance,
    voltage_filled,
    site_zone,
    distinct_site_id_in_quality_site_zone,
    voltage_filled_rank,
    last_kwh_distance,
    kwh_vs_quality_site_zone_avg,
    voltage_filled_max_2_prior,
    kwh_rank,
    quality_site_zone_avg_voltage,
    distinct_site_id_in_site_zone,
    kwh_dense_rank
FROM ranked
ORDER BY kwh_rank NULLS LAST, voltage_filled_max_2_prior, id
LIMIT 20
