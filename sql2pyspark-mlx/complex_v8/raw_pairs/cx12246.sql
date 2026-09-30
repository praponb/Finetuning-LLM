WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE quality IS NOT NULL
),
site_id_stats AS (
    -- Avg voltage per site_id
    SELECT site_id, AVG(voltage) AS avg_voltage
    FROM recent_readings
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage,
        ROUND(s.voltage - g.avg_voltage, 2) AS voltage_minus_avg
    FROM recent_readings s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage > g.avg_voltage
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE m.quality IS NOT NULL) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count
    FROM compared s
),
grouped AS (
    -- Summarise per group
    SELECT
        site_id,
        quality,
        ROUND(AVG(voltage_minus_avg), 2) AS avg_voltage_minus_avg,
        MIN(read_at) AS first_read_at
    FROM with_counts
    GROUP BY 1, 2
),
labeled AS (
    -- Label and score rows
    SELECT site_id, quality, LENGTH(TRIM(quality)) AS quality_len
    FROM grouped
)
-- Final projection
SELECT
    site_id,
    quality,
    quality_len,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug_2
FROM labeled
