WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        CASE WHEN kwh >= 83.8 THEN 'large' WHEN kwh >= 42.1 THEN 'medium' ELSE 'small' END AS kwh_tier,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM valid_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND MONTH(m.read_at) IN (1, 3)
              AND m.quality IN ('OK', 'EST')
        )
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(voltage) OVER (PARTITION BY quality_len, site_id ORDER BY voltage NULLS LAST, id) AS prev_voltage,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality_len, site_id ORDER BY voltage NULLS LAST), 3) AS voltage_percent_rank
    FROM with_activity
),
latest AS (
    -- Keep the latest row per kwh_tier
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        kwh_tier,
        quality_len,
        prev_voltage,
        voltage_percent_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY kwh_tier ORDER BY prev_voltage DESC, id DESC) AS rn
        FROM windowed
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        QUARTER(read_at) AS read_at_quarter,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM latest
    WHERE kwh_tier IN ('small', 'medium')
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        kwh_tier,
        quality_len,
        prev_voltage,
        voltage_percent_rank,
        read_at_label,
        quality_group,
        ABS(voltage - 222.6) AS voltage_distance
    FROM enriched
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_voltage,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM enriched_2 s
)
SELECT id, metric_name, value
FROM (
    SELECT id, voltage_percent_rank, prev_voltage, voltage
    FROM with_counts
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (voltage_percent_rank, prev_voltage, voltage)
) unp
