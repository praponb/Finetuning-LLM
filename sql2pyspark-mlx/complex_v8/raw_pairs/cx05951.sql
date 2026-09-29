WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        CASE WHEN kwh >= 83.8 THEN 'large' WHEN kwh >= 30.2 THEN 'medium' ELSE 'small' END AS kwh_tier
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-02 11:00:00'
      AND voltage > 215.1
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(voltage) OVER (PARTITION BY site_id, quality) AS site_id_quality_max_voltage,
        COUNT(*) OVER (PARTITION BY site_id, quality) AS rows_in_site_id_quality,
        SIZE(COLLECT_SET(kwh_tier) OVER (PARTITION BY site_id, quality)) AS distinct_kwh_tier_in_site_id_quality
    FROM filtered
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality_or_unknown,
        s.kwh_tier,
        s.site_id_quality_max_voltage,
        s.rows_in_site_id_quality,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at
    FROM sequenced s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh_tier,
    site_id_quality_max_voltage,
    rows_in_site_id_quality,
    reading_id,
    reading_kwh,
    reading_voltage,
    reading_read_at
FROM detail_rows_readings
ORDER BY site_id_quality_max_voltage
