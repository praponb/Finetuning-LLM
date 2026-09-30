WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
combined_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        si.zone AS site_zone
    FROM valid s
    LEFT JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_all_by_read_at)) / 60, 1) AS minutes_since_prev_read_at,
        ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY kwh, id) AS kwh_seq,
        COUNT(*) OVER w_quality_site_zone AS rows_in_quality_site_zone,
        MIN(kwh) OVER (PARTITION BY quality, site_zone) AS quality_site_zone_min_kwh,
        MIN(kwh) OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_so_far,
        ROUND(AVG(kwh) OVER (PARTITION BY quality, site_zone), 2) AS quality_site_zone_avg_kwh,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality, site_zone), 2) AS kwh_vs_quality_site_zone_avg,
        FIRST_VALUE(read_at) OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality, site_zone), 0), 4) AS kwh_share_of_quality_site_zone
    FROM combined_details
    WINDOW w_all_by_read_at AS (ORDER BY read_at, id), w_quality_site_zone AS (PARTITION BY quality, site_zone)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    site_zone,
    minutes_since_prev_read_at,
    kwh_seq,
    rows_in_quality_site_zone,
    quality_site_zone_min_kwh,
    min_kwh_so_far,
    first_read_at,
    kwh_share_of_quality_site_zone,
    first_read_at + INTERVAL 30 MINUTES AS first_read_at_plus_30m
FROM metrics
ORDER BY rows_in_quality_site_zone DESC
