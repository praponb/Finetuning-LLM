WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled,
        CASE WHEN quality IN ('EST', 'OK') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE (quality = 'OK' OR kwh >= 29.2)
),
with_parent AS (
    -- Attach site attributes
    SELECT s.*, si.zone AS site_zone
    FROM scoped_readings s
    JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        voltage - LAG(voltage) OVER (ORDER BY read_at, id) AS voltage_change,
        ROUND(kwh_filled / NULLIF(SUM(kwh_filled) OVER w_quality_group_site_zone, 0), 4) AS kwh_filled_share_of_quality_group_site_zone,
        MAX(kwh_filled) OVER (PARTITION BY quality_group, site_zone) AS quality_group_site_zone_max_kwh_filled,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality_group, site_zone), 2) AS kwh_vs_quality_group_site_zone_avg,
        ROUND(AVG(kwh_filled) OVER (PARTITION BY quality_group, site_zone), 2) AS quality_group_site_zone_avg_kwh_filled,
        LEAD(read_at, 2) OVER (ORDER BY kwh_filled ASC, id) AS next2_read_at,
        MAX(kwh) OVER (PARTITION BY quality_group, site_zone) AS quality_group_site_zone_max_kwh,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh_filled ASC, id), 3) AS kwh_filled_percent_rank
    FROM with_parent
    WINDOW w_quality_group_site_zone AS (PARTITION BY quality_group, site_zone)
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh_filled,
        quality_group,
        site_zone,
        quality_group_site_zone_max_kwh_filled,
        kwh_vs_quality_group_site_zone_avg,
        quality_group_site_zone_avg_kwh_filled,
        quality_group_site_zone_max_kwh,
        DATE_FORMAT(next2_read_at, 'yyyy-MM-dd') AS next2_read_at_label
    FROM windowed
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        kwh_filled,
        quality_group,
        site_zone,
        kwh_vs_quality_group_site_zone_avg,
        quality_group_site_zone_avg_kwh_filled,
        quality_group_site_zone_max_kwh,
        next2_read_at_label,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id, quality), 2) AS voltage_vs_site_id_quality_avg,
        RANK() OVER (PARTITION BY site_id, quality_group ORDER BY kwh, id) AS kwh_rank,
        ROUND(voltage - AVG(voltage) OVER w_quality, 2) AS voltage_vs_quality_avg,
        LEAD(kwh, 2) OVER (PARTITION BY site_id, quality_group ORDER BY kwh, id) AS next2_kwh,
        SIZE(COLLECT_SET(site_zone) OVER (PARTITION BY site_id, quality)) AS distinct_site_zone_in_site_id_quality,
        RANK() OVER (PARTITION BY site_id ORDER BY voltage, id) AS voltage_rank,
        RANK() OVER w_quality_group_by_voltage_tie AS voltage_rank_2,
        MAX(voltage) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_max_3
    FROM labeled
    WINDOW w_quality AS (PARTITION BY quality), w_quality_group_by_voltage_tie AS (PARTITION BY quality_group ORDER BY voltage ASC)
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    voltage,
    kwh_filled,
    quality_group,
    site_zone,
    quality_group_site_zone_avg_kwh_filled,
    quality_group_site_zone_max_kwh,
    next2_read_at_label,
    kwh_rank,
    next2_kwh,
    distinct_site_zone_in_site_id_quality,
    voltage_rank,
    voltage_rank_2,
    voltage_max_3
FROM sequenced
ORDER BY site_zone, distinct_site_zone_in_site_id_quality DESC NULLS FIRST
