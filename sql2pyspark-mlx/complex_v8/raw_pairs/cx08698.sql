WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 104.8 THEN 'A' WHEN kwh >= 51.0 THEN 'B' ELSE 'C' END AS kwh_tier
    FROM meter_readings
    WHERE quality <> 'EST'
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(read_at) AS last_reading_read_at,
        MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at,
        m.first_reading_read_at
    FROM filtered s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
qualified AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.commissioned_on BETWEEN DATE '2024-09-15' AND DATE '2025-11-28'
              AND si.installed_kw < 284.0
        )
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at_2
    FROM qualified s
),
latest AS (
    -- Keep the earliest row per site_id, quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh_tier,
        last_reading_read_at,
        first_reading_read_at,
        last_reading_read_at_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY voltage, id) AS rn
        FROM with_stats
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    read_at,
    kwh,
    quality,
    kwh_tier,
    first_reading_read_at,
    DATE_FORMAT(last_reading_read_at, 'yyyy-MM-dd') AS last_reading_read_at_label
FROM latest
ORDER BY kwh_tier, quality
