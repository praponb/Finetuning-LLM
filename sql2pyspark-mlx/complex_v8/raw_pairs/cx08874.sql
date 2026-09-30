WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage < 232.0
      AND read_at < TIMESTAMP '2025-02-12 08:00:00'
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM scoped_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'BAD'
        )
),
enriched_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.quality,
        si.site_name
    FROM screened s
    LEFT JOIN sites si ON si.id = s.site_id
),
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM enriched_site s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND LENGTH(TRIM(si.site_name)) >= 6
        )
),
summary AS (
    -- Summarise per group
    SELECT
        quality,
        site_id,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM filtered
    GROUP BY 1, 2
)
-- Final projection
SELECT
    quality,
    site_id,
    cnt,
    quality_list,
    LENGTH(TRIM(quality)) AS quality_len,
    LENGTH(TRIM(quality)) AS quality_len_2
FROM summary
WHERE cnt > 5
ORDER BY quality, quality_len, site_id
LIMIT 20
