WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh >= 20.4
      AND site_id IS NOT NULL
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        si.site_name,
        si.zone AS site_zone
    FROM recent s
    JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY read_at DESC, id) AS read_at_seq,
        LEAD(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS next_read_at,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_site_zone_site_id_by_read_at)) / 60, 1) AS minutes_since_prev_read_at,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY read_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        LEAD(read_at) OVER (ORDER BY read_at DESC, id) AS next_read_at_2,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY read_at), 3) AS read_at_cume_dist,
        NTILE(4) OVER (ORDER BY read_at DESC, id) AS read_at_quartile,
        ROUND(CUME_DIST() OVER (ORDER BY read_at DESC, id), 3) AS read_at_cume_dist_2
    FROM with_details_site
    WINDOW w_site_zone_site_id_by_read_at AS (PARTITION BY site_zone, site_id ORDER BY read_at DESC, id), w_all_by_read_at AS (ORDER BY read_at DESC, id)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE read_at_seq <= 2
      AND minutes_since_prev_read_at > 2000.0
)
-- Final projection
SELECT
    id,
    read_at,
    quality,
    site_name,
    minutes_since_prev_read_at,
    read_at_quartile,
    read_at_cume_dist_2,
    COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
FROM first_rows
