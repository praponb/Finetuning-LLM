WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ABS(kwh - 84.9) AS kwh_distance,
        ABS(voltage - 227.1) AS voltage_distance
    FROM meter_readings
    WHERE kwh <= 81.9
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.kwh_distance,
        si.tariff AS site_tariff,
        si.zone AS site_zone,
        si.site_name
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff IN ('TOU', 'FLAT')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY site_zone ORDER BY read_at NULLS LAST, id) AS read_at_rank,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY quality, site_id ORDER BY kwh_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        SIZE(COLLECT_SET(site_zone) OVER (PARTITION BY site_tariff, site_id)) AS distinct_site_zone_in_site_tariff_site_id,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY kwh_distance ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage,
        ROUND(CUME_DIST() OVER w_quality_site_id_by_kwh_dista_2, 3) AS kwh_distance_cume_dist,
        LEAD(voltage, 1, 0) OVER w_all_by_kwh_distance AS next_voltage,
        ROUND(AVG(kwh_distance) OVER (PARTITION BY quality, site_id ORDER BY kwh_distance ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS kwh_distance_avg_2_prior,
        MIN(kwh_distance) OVER w_site_tariff AS site_tariff_min_kwh_distance,
        LAST_VALUE(voltage) OVER (ORDER BY kwh_distance ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_zone ORDER BY read_at NULLS LAST), 3) AS read_at_percent_rank,
        ROUND(kwh_distance - AVG(kwh_distance) OVER (PARTITION BY site_tariff), 2) AS kwh_distance_vs_site_tariff_avg
    FROM enriched
    WINDOW w_quality_site_id_by_kwh_dista_2 AS (PARTITION BY quality, site_id ORDER BY kwh_distance ASC), w_all_by_kwh_distance AS (ORDER BY kwh_distance ASC NULLS LAST, id), w_site_tariff AS (PARTITION BY site_tariff)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        kwh_distance,
        site_tariff,
        site_zone,
        site_name,
        last_known_read_at,
        distinct_site_zone_in_site_tariff_site_id,
        first_known_voltage,
        kwh_distance_cume_dist,
        next_voltage,
        kwh_distance_avg_2_prior,
        site_tariff_min_kwh_distance,
        last_voltage,
        read_at_percent_rank,
        kwh_distance_vs_site_tariff_avg
    FROM sequenced
    WHERE read_at_rank = 1
      AND kwh_distance_vs_site_tariff_avg >= -24.0
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        site_name,
        last_known_read_at,
        first_known_voltage,
        last_voltage,
        read_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY quality ORDER BY next_voltage DESC), 3) AS next_voltage_cume_dist,
        ROW_NUMBER() OVER (ORDER BY voltage, id) AS rn,
        FIRST_VALUE(next_voltage) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_next_voltage,
        ROUND(first_known_voltage / NULLIF(SUM(first_known_voltage) OVER w_site_zone_distinct_site_zone, 0), 4) AS first_known_voltage_share_of_site_zone_distinct_,
        LAG(first_known_voltage) OVER (PARTITION BY site_id, quality ORDER BY first_known_voltage DESC, id) AS prev_first_known_voltage
    FROM first_rows
    WINDOW w_site_zone_distinct_site_zone AS (PARTITION BY site_zone, distinct_site_zone_in_site_tariff_site_id)
)
SELECT
    id,
    site_id,
    voltage,
    quality,
    site_name,
    first_known_voltage,
    last_voltage,
    read_at_percent_rank,
    next_voltage_cume_dist,
    rn,
    first_next_voltage,
    prev_first_known_voltage
FROM metrics
ORDER BY last_voltage, prev_first_known_voltage
