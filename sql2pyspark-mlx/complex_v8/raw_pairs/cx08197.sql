WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE site_id >= 5
      AND read_at < TIMESTAMP '2025-02-18 12:00:00'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        quality_ref,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh,
        kwh - voltage AS kwh_minus_voltage
    FROM recent_readings
),
quality_stats AS (
    -- Avg kwh_minus_voltage per quality
    SELECT quality, AVG(kwh_minus_voltage) AS avg_kwh_minus_voltage
    FROM labeled
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_kwh_minus_voltage,
        ROUND(s.kwh_minus_voltage - g.avg_kwh_minus_voltage, 2) AS kwh_minus_voltage_minus_avg
    FROM labeled s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh_minus_voltage > g.avg_kwh_minus_voltage
),
summary AS (
    -- Group-level totals
    SELECT
        quality,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        MIN(avg_kwh_minus_voltage) AS min_avg_kwh_minus_voltage,
        MIN(read_at) AS first_read_at
    FROM above_average
    GROUP BY 1, 2
    HAVING AVG(voltage) > 215.6
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        quality,
        read_at_month,
        min_avg_kwh_minus_voltage,
        FIRST_VALUE(min_avg_kwh_minus_voltage) OVER (PARTITION BY MONTH(first_read_at) ORDER BY min_avg_kwh_minus_voltage, quality, read_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_min_avg_kwh_minus_voltage,
        ROUND(CUME_DIST() OVER w_all_by_first_read_at, 3) AS first_read_at_cume_dist
    FROM summary
    WINDOW w_all_by_first_read_at AS (ORDER BY first_read_at DESC, quality, read_at_month)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        quality,
        read_at_month,
        first_min_avg_kwh_minus_voltage,
        ROUND((UNIX_TIMESTAMP(read_at_month) - UNIX_TIMESTAMP(LAG(read_at_month) OVER w_quality_by_first_min_avg_kwh)) / 60, 1) AS minutes_since_prev_read_at_month,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        ROUND(min_avg_kwh_minus_voltage - AVG(min_avg_kwh_minus_voltage) OVER (PARTITION BY quality), 2) AS min_avg_kwh_minus_voltage_vs_quality_avg,
        LAG(first_read_at_cume_dist) OVER w_all_by_first_min_avg_kwh_min AS prev_first_read_at_cume_dist,
        FIRST_VALUE(read_at_month) OVER (PARTITION BY quality ORDER BY first_min_avg_kwh_minus_voltage DESC, quality, read_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at_month
    FROM ranked
    WINDOW w_quality_by_first_min_avg_kwh AS (PARTITION BY quality ORDER BY first_min_avg_kwh_minus_voltage DESC, quality, read_at_month), w_all_by_first_min_avg_kwh_min AS (ORDER BY first_min_avg_kwh_minus_voltage ASC, quality, read_at_month)
)
-- Final projection
SELECT
    quality,
    first_min_avg_kwh_minus_voltage,
    minutes_since_prev_read_at_month,
    rows_in_quality,
    min_avg_kwh_minus_voltage_vs_quality_avg,
    first_read_at_month,
    read_at_month + INTERVAL 1 DAYS AS read_at_month_plus_1d,
    CONCAT(quality, '#', CAST(rows_in_quality AS STRING)) AS quality_ref
FROM metrics
ORDER BY quality
