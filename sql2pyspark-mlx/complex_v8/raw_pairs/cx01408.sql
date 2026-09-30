WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        TO_DATE(read_at) AS read_at_date,
        ABS(kwh - 63.4) AS kwh_distance,
        CASE WHEN quality IN ('OK', 'BAD') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE kwh > 30.2
      AND quality IN ('OK', 'BAD')
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.read_at_date,
        s.quality_group,
        si.zone AS site_zone,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.site_zone,
        s.site_tariff,
        s.site_installed_kw,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality
    FROM with_details_site s
    JOIN meter_readings m ON m.site_id = s.site_id
),
combined_details_reading AS (
    -- Join meter_readings
    SELECT
        s.id,
        s.site_id,
        s.site_zone,
        s.site_installed_kw,
        s.reading_id,
        s.reading_kwh,
        s.reading_quality,
        m.quality AS reading_quality_2,
        si.commissioned_on AS site_commissioned_on
    FROM detail_rows_readings s
    JOIN meter_readings m ON m.id = s.reading_id
    LEFT JOIN sites si ON si.id = m.site_id
    WHERE m.quality IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(site_installed_kw) OVER (PARTITION BY reading_quality) AS reading_quality_max_site_installed_kw,
        COUNT(*) OVER (PARTITION BY reading_quality) AS rows_in_reading_quality,
        MIN(reading_kwh) OVER (ORDER BY site_commissioned_on DESC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_reading_kwh_so_far,
        SIZE(COLLECT_SET(reading_quality_2) OVER (PARTITION BY reading_quality)) AS distinct_reading_quality_2_in_reading_quality
    FROM combined_details_reading
)
-- Final projection
SELECT
    id,
    site_id,
    site_zone,
    site_installed_kw,
    reading_id,
    reading_kwh,
    reading_quality,
    reading_quality_2,
    site_commissioned_on,
    reading_quality_max_site_installed_kw,
    rows_in_reading_quality,
    min_reading_kwh_so_far,
    distinct_reading_quality_2_in_reading_quality,
    LOWER(TRIM(reading_quality)) AS reading_quality_clean,
    ABS(site_installed_kw - 141.6) AS site_installed_kw_distance
FROM sequenced
WHERE site_zone <> 'Z4'
