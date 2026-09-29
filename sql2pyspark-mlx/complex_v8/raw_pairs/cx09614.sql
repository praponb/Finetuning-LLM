WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 86.0 THEN 'A' WHEN kwh >= 52.4 THEN 'B' ELSE 'C' END AS kwh_tier,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE (read_at < TIMESTAMP '2025-02-24 14:00:00' OR quality IN ('EST', 'OK'))
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM filtered s
    LEFT JOIN sites si ON si.id = s.site_id
),
filtered_2 AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM with_parent_site s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.commissioned_on < DATE '2025-05-10'
        )
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at
    FROM filtered_2 s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    kwh_tier,
    site_installed_kw,
    site_tariff,
    site_zone,
    first_reading_read_at,
    LOWER(REGEXP_REPLACE(TRIM(site_tariff), ' ', '_')) AS site_tariff_slug,
    ABS(kwh - 74.3) AS kwh_distance
FROM with_counts
WHERE kwh < 78.6
ORDER BY kwh, kwh_distance DESC, id
LIMIT 10
