WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LEAST(voltage, kwh) AS least_voltage_kwh
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM recent_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN voltage >= 238.3 THEN 'large' WHEN voltage >= 223.1 THEN 'medium' ELSE 'small' END AS voltage_tier,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        ROUND(SUM(new_session_flag) / NULLIF(SUM(least_voltage_kwh), 0), 3) AS new_session_flag_to_least_voltage_kwh_ratio,
        MAX(voltage) AS max_voltage,
        SUM(new_session_flag) FILTER (WHERE quality NOT IN ('BAD', 'OK')) AS new_session_flag_quality_not_in_bad_ok,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(kwh, 0.5) AS median_kwh,
        ROUND(STDDEV(least_voltage_kwh), 2) AS stddev_least_voltage_kwh
    FROM sessionized
    WHERE new_session_flag < 1
    GROUP BY 1, 2
    HAVING SUM(new_session_flag) > 0
)
-- Final projection
SELECT
    voltage_tier,
    read_at_month,
    new_session_flag_quality_not_in_bad_ok,
    distinct_ids,
    COALESCE(stddev_least_voltage_kwh, 0.0) AS stddev_least_voltage_kwh_filled,
    COALESCE(voltage_tier, 'UNKNOWN') AS voltage_tier_or_unknown
FROM per_group
WHERE distinct_ids BETWEEN 4 AND 8
ORDER BY stddev_least_voltage_kwh_filled, distinct_ids
