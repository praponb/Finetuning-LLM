WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, ROUND(AVG(kwh), 2) AS avg_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_kwh
    FROM prepared_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
unique_rows AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality_slug,
        avg_reading_kwh
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh DESC, id DESC) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality_slug,
        si.commissioned_on AS site_commissioned_on,
        si.site_name,
        si.installed_kw AS site_installed_kw
    FROM unique_rows s
    LEFT JOIN sites si ON si.id = s.site_id AND si.site_name IS NOT NULL
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM combined_details_site s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage >= 215.8
              AND MONTH(m.read_at) IN (1, 3)
        )
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality_slug,
    site_commissioned_on,
    site_name,
    ROUND(site_installed_kw / NULLIF(kwh, 0), 3) AS site_installed_kw_per_kwh,
    DATE_TRUNC('YEAR', site_commissioned_on) AS site_commissioned_on_year
FROM without_matches
WHERE kwh < 110.5
ORDER BY site_installed_kw_per_kwh, kwh DESC
