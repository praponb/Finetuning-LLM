WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM meter_readings
),
with_parent AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.days_since_read_at,
        si.tariff AS site_tariff,
        si.zone AS site_zone,
        si.site_name
    FROM filtered s
    JOIN sites si ON si.id = s.site_id AND si.installed_kw >= 93.3
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT MAX(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_kwh
    FROM with_parent s
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        max_reading_kwh - LAG(max_reading_kwh) OVER (ORDER BY last_reading_read_at, id) AS max_reading_kwh_change,
        ROUND(max_reading_kwh / NULLIF(SUM(max_reading_kwh) OVER w_site_id_quality, 0), 4) AS max_reading_kwh_share_of_site_id_quality
    FROM profiled
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
),
with_details_site AS (
    -- Attach site attributes
    SELECT s.*, si.zone AS site_zone_2
    FROM sequenced s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY site_zone ORDER BY max_reading_kwh_change ASC, id) AS max_reading_kwh_change_dense_rank,
        RANK() OVER (PARTITION BY site_zone ORDER BY max_reading_kwh_change ASC, id) AS max_reading_kwh_change_rank,
        NTILE(10) OVER (PARTITION BY site_zone ORDER BY max_reading_kwh_change ASC, id) AS max_reading_kwh_change_decile,
        SIZE(COLLECT_SET(site_zone_2) OVER w_site_id) AS distinct_site_zone_2_in_site_id,
        SUM(kwh) OVER (PARTITION BY site_zone ORDER BY max_reading_kwh_change ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh
    FROM with_details_site
    WINDOW w_site_id AS (PARTITION BY site_id)
)
-- Final projection
SELECT
    id,
    site_id,
    quality,
    days_since_read_at,
    site_zone,
    site_name,
    last_reading_read_at,
    max_reading_kwh,
    max_reading_kwh_change,
    max_reading_kwh_share_of_site_id_quality,
    max_reading_kwh_change_rank,
    running_kwh,
    LENGTH(TRIM(site_zone_2)) AS site_zone_2_len,
    CONCAT(site_tariff, '#', CAST(site_id AS STRING)) AS site_tariff_ref
FROM with_trends
