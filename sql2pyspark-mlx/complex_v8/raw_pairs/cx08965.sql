WITH cleaned_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        tariff,
        commissioned_on,
        DATE_TRUNC('MONTH', commissioned_on) AS commissioned_on_month,
        CASE WHEN zone IN ('Z3', 'Z2') THEN 'group_a' WHEN zone = 'Z4' THEN 'group_b' ELSE 'other' END AS zone_group
    FROM sites
    WHERE zone = 'Z2'
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', commissioned_on_month) AS days_since_commissioned_on_month,
        LOWER(REGEXP_REPLACE(TRIM(zone_group), ' ', '_')) AS zone_group_slug,
        LENGTH(TRIM(zone_group)) AS zone_group_len
    FROM cleaned_sites
    WHERE zone_group IN ('group_b', 'group_a')
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM labeled
    WHERE zone_group_len > (SELECT MIN(zone_group_len) FROM labeled)
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at
    FROM qualified s
    LEFT JOIN reading_stats m ON m.site_id = s.id
)
-- Final projection
SELECT
    id,
    tariff,
    commissioned_on_month,
    zone_group,
    last_reading_read_at,
    HOUR(commissioned_on_month) AS commissioned_on_month_hour,
    COALESCE(zone_group, 'UNKNOWN') AS zone_group_or_unknown
FROM with_stats
ORDER BY commissioned_on_month DESC
