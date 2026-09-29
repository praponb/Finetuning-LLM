WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        DATE_TRUNC('DAY', read_at) AS read_at_day
    FROM meter_readings
    WHERE voltage > 218.2
      AND TO_DATE(read_at) < DATE '2025-02-06'
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM filtered_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh > 28.4
              AND m.voltage BETWEEN 215.8 AND 235.8
        )
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(kwh, 2) OVER (PARTITION BY TO_DATE(read_at) ORDER BY voltage NULLS LAST, id) AS next2_kwh,
        NTILE(5) OVER (PARTITION BY site_id ORDER BY days_since_read_at DESC, id) AS days_since_read_at_quintile,
        MAX(reading_count) OVER (PARTITION BY site_id, TO_DATE(read_at)) AS site_id_read_at_to_date_max_reading_count
    FROM with_activity
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.days_since_read_at,
        s.read_at_day,
        s.next2_kwh,
        s.days_since_read_at_quintile,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM with_trends s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone NOT IN ('Z3', 'Z4')
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        COUNT(CASE WHEN quality NOT IN ('BAD', 'EST') THEN 1 END) AS reading_quality_count,
        ROUND(AVG(voltage), 2) AS avg_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        m.avg_reading_voltage
    FROM joined s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.site_id
)
SELECT
    id,
    site_id,
    read_at,
    days_since_read_at,
    read_at_day,
    days_since_read_at_quintile,
    site_commissioned_on,
    site_zone,
    reading_count,
    reading_quality_count,
    avg_reading_voltage,
    ROUND(voltage * 1.5, 2) AS voltage_adj
FROM summarized_rows
ORDER BY read_at, days_since_read_at
