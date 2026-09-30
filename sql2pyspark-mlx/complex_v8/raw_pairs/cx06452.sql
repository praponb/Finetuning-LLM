WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        HOUR(read_at) AS read_at_hour,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE kwh > 45.0
      AND voltage >= 218.2
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM valid s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
        )
      AND s.quality_len <= 12
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT MIN(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_voltage,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM screened s
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count_2,
        MAX(read_at) AS last_reading_read_at_2,
        COUNT(*) FILTER (WHERE quality = 'BAD') AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count_2, 0) AS reading_count_2,
        m.last_reading_read_at_2,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM with_counts s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.site_id
),
reading_stats_3 AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(voltage) AS max_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.max_reading_voltage
    FROM summarized_rows s
    LEFT JOIN reading_stats_3 m ON m.site_id = s.site_id
),
unique_rows AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        read_at_month,
        read_at_hour,
        quality_len,
        reading_count,
        min_reading_voltage,
        last_reading_read_at,
        reading_count_2,
        last_reading_read_at_2,
        reading_quality_count,
        max_reading_voltage
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY reading_count_2 DESC, id DESC) AS recency_rank
        FROM profiled
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    read_at_hour,
    min_reading_voltage,
    last_reading_read_at,
    reading_count_2,
    reading_quality_count,
    max_reading_voltage,
    DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
    ROUND(max_reading_voltage * 1.1, 2) AS max_reading_voltage_adj
FROM unique_rows
ORDER BY read_at, min_reading_voltage
