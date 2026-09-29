WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        ABS(kwh - 78.6) AS kwh_distance
    FROM meter_readings
    WHERE (voltage < 238.3 OR read_at >= TIMESTAMP '2025-01-25 11:00:00')
),
site_id_stats AS (
    -- Avg kwh per site_id
    SELECT site_id, AVG(kwh) AS avg_kwh
    FROM recent
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_kwh, ROUND(s.kwh - g.avg_kwh, 2) AS kwh_minus_avg
    FROM recent s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh > g.avg_kwh
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM vs_group s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at >= TIMESTAMP '2025-01-19 13:00:00'
              AND m.voltage < 234.5
        )
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(read_at) AS first_reading_read_at,
        COUNT(CASE WHEN quality NOT IN ('BAD', 'OK') THEN 1 END) AS reading_quality_count,
        COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM with_activity s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, avg_kwh, kwh_distance, kwh_minus_avg, kwh
    FROM summarized_rows
) src
UNPIVOT (
    value FOR metric_name IN (avg_kwh, kwh_distance, kwh_minus_avg, kwh)
) unp
