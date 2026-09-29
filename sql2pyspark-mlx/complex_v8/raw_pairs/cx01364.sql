WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE site_id >= 8
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM valid_readings s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE HOUR(m.read_at) BETWEEN 8 AND 11
              AND m.kwh BETWEEN 37.0 AND 109.0
        )
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM qualified s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
totals AS (
    -- Summarise per group
    SELECT
        reading_count,
        ROUND(AVG(voltage), 2) AS avg_voltage,
        SUM(kwh) AS total_kwh,
        SUM(CASE WHEN quality NOT IN ('OK', 'EST') THEN reading_count ELSE 0 END) AS reading_count_quality_not_in_ok_est,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        ROUND(SUM(voltage), 2) AS total_voltage,
        SUM(reading_count) AS total_reading_count
    FROM profiled
    WHERE reading_count > 3
    GROUP BY reading_count
    HAVING MAX(voltage) > 220.2
),
derived AS (
    -- Add calculated columns
    SELECT
        reading_count,
        total_kwh,
        reading_count_quality_not_in_ok_est,
        quality_list,
        total_voltage,
        total_reading_count,
        ABS(total_voltage - 1092.0) AS total_voltage_distance
    FROM totals
    WHERE avg_voltage <= 239.8
),
windowed AS (
    -- Previous / next values per key
    SELECT
        reading_count,
        total_kwh,
        reading_count_quality_not_in_ok_est,
        quality_list,
        total_voltage,
        total_reading_count,
        total_voltage_distance,
        FIRST_VALUE(reading_count) OVER (PARTITION BY reading_count ORDER BY reading_count_quality_not_in_ok_est DESC, reading_count ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reading_count,
        ROUND(reading_count - AVG(reading_count) OVER (PARTITION BY reading_count), 2) AS reading_count_vs_reading_count_avg,
        ROW_NUMBER() OVER (PARTITION BY reading_count ORDER BY total_voltage, reading_count) AS total_voltage_seq,
        LEAD(total_voltage_distance) OVER (PARTITION BY reading_count ORDER BY total_voltage, reading_count) AS next_total_voltage_distance
    FROM derived
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN total_kwh >= 540.0 THEN 'A' WHEN total_kwh >= 216.0 THEN 'B' ELSE 'C' END AS total_kwh_tier
    FROM windowed
    WHERE total_voltage <= 1092.0
)
-- Final projection
SELECT
    reading_count,
    total_kwh,
    reading_count_quality_not_in_ok_est,
    quality_list,
    total_voltage,
    total_reading_count,
    total_voltage_distance,
    first_reading_count,
    total_voltage_seq,
    next_total_voltage_distance,
    total_kwh_tier
FROM enriched
WHERE reading_count_vs_reading_count_avg > -6.4
ORDER BY total_voltage DESC, total_voltage_distance DESC, reading_count
LIMIT 10
