WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_voltage,
        (SELECT ROUND(AVG(m.kwh), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_kwh,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM recent s
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        max_reading_voltage,
        avg_reading_kwh,
        reading_count,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM profiled
    WHERE max_reading_voltage >= 224.0
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        avg_reading_kwh,
        reading_count,
        days_since_read_at,
        quality_slug,
        LENGTH(TRIM(quality)) AS quality_len
    FROM labeled
),
quality_stats AS (
    -- Max quality_len per quality
    SELECT quality, MAX(quality_len) AS max_quality_len
    FROM tagged
    GROUP BY quality
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_quality_len,
        ROUND(s.quality_len - g.max_quality_len, 2) AS quality_len_minus_max
    FROM tagged s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.quality_len = g.max_quality_len
),
site_id_stats AS (
    -- Avg days_since_read_at per site_id
    SELECT site_id, AVG(days_since_read_at) AS avg_days_since_read_at
    FROM outliers
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_read_at,
        ROUND(s.days_since_read_at - g.avg_days_since_read_at, 2) AS days_since_read_at_minus_avg
    FROM outliers s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.days_since_read_at > g.avg_days_since_read_at
),
rollup_base AS (
    -- Summarise per group
    SELECT reading_count, quality, MIN(read_at) AS first_read_at
    FROM compared
    GROUP BY reading_count, quality
)
-- Final projection
SELECT
    reading_count,
    quality,
    first_read_at,
    COALESCE(reading_count, 0) AS reading_count_filled,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
FROM rollup_base
ORDER BY quality_slug
