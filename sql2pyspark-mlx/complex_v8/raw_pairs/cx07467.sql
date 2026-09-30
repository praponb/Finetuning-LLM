WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE quality NOT IN ('OK', 'BAD')
      AND site_id IS NOT NULL
),
latest AS (
    -- Keep the latest row per quality, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY voltage DESC, id DESC) AS row_num
        FROM valid
    ) ranked
    WHERE row_num = 1
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        site_id,
        COUNT(*) AS cnt,
        SUM(voltage) FILTER (WHERE quality = 'BAD') AS voltage_quality_bad,
        COUNT(DISTINCT quality) AS distinct_qualitys
    FROM latest
    WHERE kwh > 48.2
    GROUP BY DATE_TRUNC('MONTH', read_at), site_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(CASE WHEN quality = 'OK' THEN 1 END) AS reading_quality_count,
        MAX(kwh) AS max_reading_kwh,
        COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        m.max_reading_kwh,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM grouped s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM with_counts
    WHERE voltage_quality_bad < (SELECT MAX(voltage_quality_bad) FROM with_counts)
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.read_at_month,
        s.site_id,
        s.cnt,
        s.voltage_quality_bad,
        s.distinct_qualitys,
        s.max_reading_kwh,
        s.reading_count,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage
    FROM with_activity s
    JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    read_at_month,
    site_id,
    cnt,
    voltage_quality_bad,
    distinct_qualitys,
    max_reading_kwh,
    reading_count,
    reading_id,
    reading_read_at
FROM with_children_readings
ORDER BY reading_count DESC, site_id DESC
