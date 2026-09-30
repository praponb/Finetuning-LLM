WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id >= 2
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
    FROM base
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        MAX(read_at) AS last_reading_read_at,
        COUNT(CASE WHEN quality IN ('BAD', 'OK') THEN 1 END) AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.last_reading_read_at,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM derived s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
stats AS (
    -- Group-level totals
    SELECT
        quality_group,
        DATE_TRUNC('MONTH', last_reading_read_at) AS last_reading_read_at_month,
        MAX(kwh_per_voltage) AS max_kwh_per_voltage,
        SUM(reading_quality_count) AS total_reading_quality_count,
        COUNT(*) AS row_count,
        COUNT(DISTINCT quality_group) AS distinct_quality_groups,
        MAX(last_reading_read_at) AS last_last_reading_read_at
    FROM summarized_rows
    WHERE kwh > 30.2
    GROUP BY quality_group, DATE_TRUNC('MONTH', last_reading_read_at)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE distinct_quality_groups > (SELECT AVG(distinct_quality_groups) FROM stats)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(last_reading_read_at_month) - UNIX_TIMESTAMP(LAG(last_reading_read_at_month) OVER (PARTITION BY quality_group ORDER BY max_kwh_per_voltage DESC, quality_group, last_reading_read_at_month))) / 60, 1) AS minutes_since_prev_last_reading_read_at_month,
        ROUND(CUME_DIST() OVER (PARTITION BY quality_group ORDER BY max_kwh_per_voltage DESC, quality_group, last_reading_read_at_month), 3) AS max_kwh_per_voltage_cume_dist,
        ROW_NUMBER() OVER (ORDER BY last_last_reading_read_at DESC, quality_group, last_reading_read_at_month) AS rn
    FROM qualified
),
derived_2 AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(quality_group, 'UNKNOWN') AS quality_group_or_unknown,
        ROUND(max_kwh_per_voltage * 1.1, 2) AS max_kwh_per_voltage_adj
    FROM with_trends
)
-- Final projection
SELECT DISTINCT
    minutes_since_prev_last_reading_read_at_month,
    total_reading_quality_count,
    max_kwh_per_voltage
FROM derived_2
ORDER BY minutes_since_prev_last_reading_read_at_month NULLS LAST, max_kwh_per_voltage NULLS LAST
