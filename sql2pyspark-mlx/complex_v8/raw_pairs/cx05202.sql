WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        MONTH(read_at) AS read_at_month
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        ROUND(AVG(voltage), 2) AS avg_reading_voltage,
        COUNT(CASE WHEN quality = 'OK' THEN 1 END) AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_voltage,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM scoped_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        voltage,
        quality,
        read_at_month,
        avg_reading_voltage,
        LAST_VALUE(avg_reading_voltage) OVER (PARTITION BY reading_quality_count, read_at_month ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_avg_reading_voltage,
        DENSE_RANK() OVER (PARTITION BY reading_quality_count, read_at_month ORDER BY read_at DESC, id) AS read_at_dense_rank,
        MIN(reading_quality_count) OVER (PARTITION BY reading_quality_count, site_id) AS reading_quality_count_site_id_min_reading_qualit,
        DENSE_RANK() OVER w_read_at_month_by_read_at_mon AS read_at_month_dense_rank
    FROM summarized_rows
    WINDOW w_read_at_month_by_read_at_mon AS (PARTITION BY read_at_month ORDER BY read_at_month)
),
with_parent AS (
    -- Join sites
    SELECT s.*, si.commissioned_on AS site_commissioned_on
    FROM sequenced s
    JOIN sites si ON si.id = s.site_id
),
reading_stats_2 AS (
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
    FROM with_parent s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.site_id
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        quality,
        last_avg_reading_voltage,
        reading_quality_count_site_id_min_reading_qualit,
        site_commissioned_on,
        reading_count,
        COALESCE(avg_reading_voltage, 0.0) AS avg_reading_voltage_filled
    FROM profiled
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM tagged
    WHERE avg_reading_voltage_filled < (SELECT MAX(avg_reading_voltage_filled) FROM tagged)
)
-- Final projection
SELECT
    id,
    quality,
    site_commissioned_on,
    reading_count,
    avg_reading_voltage_filled,
    CASE WHEN quality IN ('BAD', 'OK') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group,
    COALESCE(last_avg_reading_voltage, 0.0) AS last_avg_reading_voltage_filled
FROM without_matches
ORDER BY avg_reading_voltage_filled DESC, last_avg_reading_voltage_filled DESC NULLS FIRST
