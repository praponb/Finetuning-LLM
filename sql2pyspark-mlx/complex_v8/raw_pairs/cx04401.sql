WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        zone,
        installed_kw,
        commissioned_on,
        DATE_TRUNC('QUARTER', commissioned_on) AS commissioned_on_quarter
    FROM sites
    WHERE commissioned_on < DATE '2025-05-07'
      AND tariff IN ('FLAT', 'TOU')
),
deduped AS (
    -- Keep the latest row per zone
    SELECT
        id,
        zone,
        installed_kw,
        commissioned_on,
        commissioned_on_quarter
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone ORDER BY installed_kw DESC, id DESC) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM deduped s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.commissioned_on,
        s.commissioned_on_quarter,
        s.reading_count,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM summarized_rows s
    JOIN meter_readings m ON m.site_id = s.id
)
-- Final projection
SELECT
    id,
    reading_count,
    reading_id,
    reading_read_at,
    reading_kwh,
    reading_quality,
    reading_voltage
FROM with_children_readings
WHERE MONTH(commissioned_on_quarter) IN (1, 2)
