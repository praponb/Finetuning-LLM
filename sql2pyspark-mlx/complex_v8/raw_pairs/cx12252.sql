WITH recent_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        installed_kw
    FROM sites
    WHERE commissioned_on >= DATE '2024-09-15'
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.zone,
        s.installed_kw,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM recent_sites s
    JOIN meter_readings m ON m.site_id = s.id AND m.quality = 'OK'
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_children_readings s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.read_at >= TIMESTAMP '2025-01-25 13:00:00'
              AND m.voltage >= 216.1
        )
      AND s.installed_kw >= 141.6
),
rollup_base AS (
    -- Summarise per group
    SELECT
        zone,
        COUNT(*) AS cnt,
        COUNT(DISTINCT reading_id) AS distinct_reading_ids,
        PERCENTILE_APPROX(installed_kw, 0.5) AS median_installed_kw,
        MAX(reading_read_at) AS last_reading_read_at
    FROM filtered
    GROUP BY zone
),
scored AS (
    -- Label and score rows
    SELECT
        zone,
        cnt,
        distinct_reading_ids,
        median_installed_kw,
        last_reading_read_at,
        last_reading_read_at + INTERVAL 1 DAYS AS last_reading_read_at_plus_1d,
        DATE_FORMAT(last_reading_read_at, 'yyyy-MM-dd HH:00') AS last_reading_read_at_label,
        CASE WHEN zone IN ('Z2', 'Z4') THEN 'group_a' WHEN zone = 'Z3' THEN 'group_b' ELSE 'other' END AS zone_group
    FROM rollup_base
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS(' / ', zone_group, zone) AS zone_group_zone_label,
        DATEDIFF(DATE '2025-06-30', last_reading_read_at) AS days_since_last_reading_read_at,
        DATEDIFF(DATE '2025-06-30', last_reading_read_at) AS days_since_last_reading_read_at_2
    FROM scored
    WHERE zone IN ('Z2', 'Z4')
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(zone_group_zone_label), ' ', '_')) AS zone_group_zone_label_slug
    FROM derived
)
-- Native SQL UNPIVOT (wide to long)
SELECT zone, metric_name, reading
FROM (
    SELECT zone, days_since_last_reading_read_at, days_since_last_reading_read_at_2
    FROM tagged
) src
UNPIVOT (
    reading FOR metric_name IN (days_since_last_reading_read_at AS days_since_last_reading_read_at_v, days_since_last_reading_read_at_2 AS m_days_since_last_reading_read_at_2)
) unp
