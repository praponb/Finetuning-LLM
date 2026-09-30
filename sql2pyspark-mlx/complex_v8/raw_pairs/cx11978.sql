WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE voltage < 241.8
),
filtered_2 AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'EST'
              AND m.kwh > 46.6
        )
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM filtered_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage BETWEEN 216.1 AND 243.7
              AND m.kwh < 82.9
        )
      AND s.quality IS NOT NULL
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.quality_slug,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM screened s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY site_commissioned_on, id), 3) AS site_commissioned_on_percent_rank
    FROM joined_site
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        kwh,
        quality,
        quality_slug,
        site_commissioned_on,
        site_zone,
        kwh_share_of_site_id,
        site_commissioned_on_percent_rank,
        LAST_DAY(site_commissioned_on) AS site_commissioned_on_month_end,
        LOWER(REGEXP_REPLACE(TRIM(quality_slug), ' ', '_')) AS quality_slug_slug
    FROM with_trends
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, SUM(kwh) AS total_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.total_reading_kwh
    FROM annotated s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    quality,
    quality_slug,
    site_commissioned_on,
    site_zone,
    site_commissioned_on_percent_rank,
    site_commissioned_on_month_end,
    quality_slug_slug,
    reading_count,
    total_reading_kwh,
    QUARTER(site_commissioned_on) AS site_commissioned_on_quarter,
    FLOOR(total_reading_kwh / 100) * 100 AS total_reading_kwh_bucket
FROM with_counts
ORDER BY id DESC
