WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE kwh <= 90.6
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.quality_ref,
        s.quality_slug,
        si.zone AS site_zone
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone = 'Z3'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        quality_ref,
        site_zone,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality, site_zone), 2) AS kwh_vs_quality_site_zone_avg,
        SIZE(COLLECT_SET(site_zone) OVER w_site_id) AS distinct_site_zone_in_site_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY kwh DESC, id), 3) AS kwh_percent_rank,
        COUNT(*) OVER (PARTITION BY quality, site_zone) AS rows_in_quality_site_zone,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY site_id ORDER BY kwh DESC, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(kwh - AVG(kwh) OVER w_site_id, 2) AS kwh_vs_site_id_avg,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id
    FROM joined_site
    WINDOW w_site_id AS (PARTITION BY site_id), w_quality_site_zone AS (PARTITION BY quality, site_zone)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY site_zone, distinct_site_zone_in_site_id)) AS distinct_site_id_in_site_zone_distinct_site_zone,
        COUNT(*) OVER (PARTITION BY site_zone, distinct_site_zone_in_site_id) AS rows_in_site_zone_distinct_site_zone_in_site_id,
        LEAD(read_at) OVER (PARTITION BY site_zone ORDER BY kwh_percent_rank ASC, id) AS next_read_at,
        LAG(kwh_percent_rank) OVER (PARTITION BY site_id, distinct_site_zone_in_site_id ORDER BY read_at, id) AS prev_kwh_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY kwh_vs_site_id_avg ASC), 3) AS kwh_vs_site_id_avg_percent_rank
    FROM ranked
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    site_zone,
    kwh_vs_quality_site_zone_avg,
    distinct_site_zone_in_site_id,
    kwh_percent_rank,
    rows_in_quality_site_zone,
    kwh_vs_site_id_avg,
    rows_in_site_id,
    distinct_site_id_in_site_zone_distinct_site_zone,
    rows_in_site_zone_distinct_site_zone_in_site_id,
    prev_kwh_percent_rank,
    kwh_vs_site_id_avg_percent_rank
FROM with_windows
ORDER BY rows_in_site_id, kwh_vs_quality_site_zone_avg DESC, id
LIMIT 20
