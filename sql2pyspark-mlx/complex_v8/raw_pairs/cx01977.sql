WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        HOUR(read_at) AS read_at_hour,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND kwh <= 81.9
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        quality_slug,
        read_at_hour,
        quality_clean,
        ROUND(read_at_hour - AVG(read_at_hour) OVER w_read_at_hour, 2) AS read_at_hour_vs_read_at_hour_avg,
        LAST_VALUE(read_at_hour) OVER (PARTITION BY quality, MONTH(read_at) ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at_hour,
        RANK() OVER (PARTITION BY quality, read_at_hour ORDER BY voltage DESC, id) AS voltage_rank
    FROM scoped_readings
    WINDOW w_read_at_hour AS (PARTITION BY read_at_hour)
),
with_details_site AS (
    -- Join sites
    SELECT s.*, si.installed_kw AS site_installed_kw
    FROM windowed s
    LEFT JOIN sites si ON si.id = s.site_id
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_details_site s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at >= TIMESTAMP '2025-01-25 11:00:00'
              AND m.voltage > 222.1
        )
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IS NOT NULL
              AND m.voltage > 214.3
        )
      AND s.read_at_hour <= 14
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    quality_slug,
    read_at_hour,
    quality_clean,
    read_at_hour_vs_read_at_hour_avg,
    voltage_rank,
    site_installed_kw
FROM screened
WHERE last_read_at_hour >= 8
