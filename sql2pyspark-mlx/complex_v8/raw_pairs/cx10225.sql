WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh > 20.4
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.commissioned_on AS site_commissioned_on,
        si.site_name,
        si.tariff AS site_tariff
    FROM filtered_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
unique_rows AS (
    -- Keep the earliest row per quality, site_id
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        site_commissioned_on,
        site_name,
        site_tariff
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY voltage, id) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        MIN(read_at) AS first_reading_read_at,
        MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.first_reading_read_at,
        m.last_reading_read_at
    FROM unique_rows s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
qualified AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM profiled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND LOWER(si.site_name) LIKE '%p %'
        )
),
joined_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.installed_kw AS site_installed_kw
    FROM qualified s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw BETWEEN 141.6 AND 373.3
)
-- Final projection
SELECT
    id,
    voltage,
    quality,
    site_tariff,
    reading_count,
    first_reading_read_at,
    last_reading_read_at,
    site_installed_kw,
    ABS(site_installed_kw - 373.3) AS site_installed_kw_distance,
    SUBSTRING(TRIM(site_tariff), 1, 4) AS site_tariff_prefix
FROM joined_site
ORDER BY first_reading_read_at DESC
