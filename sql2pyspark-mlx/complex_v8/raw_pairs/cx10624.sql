WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE kwh > 20.3
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM filtered_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
enriched AS (
    -- Derive helper columns
    SELECT *, COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM sessionized
    WHERE new_session_flag < 1
),
stats AS (
    -- Summarise per group
    SELECT
        site_id,
        SUM(CASE WHEN quality NOT IN ('EST', 'BAD') THEN 1 ELSE 0 END) AS n_quality_not_in_est_bad,
        ROUND(100 * COUNT(CASE WHEN quality <> 'OK' THEN 1 END) / COUNT(*), 1) AS pct_quality_ok,
        MIN(new_session_flag) AS min_new_session_flag,
        MAX(read_at) AS last_read_at,
        ROUND(AVG(kwh_per_voltage), 2) AS avg_kwh_per_voltage
    FROM enriched
    GROUP BY 1
)
SELECT
    site_id,
    n_quality_not_in_est_bad,
    pct_quality_ok,
    last_read_at,
    avg_kwh_per_voltage
FROM stats
