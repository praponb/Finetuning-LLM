WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage + kwh AS voltage_plus_kwh
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-03-08'
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(prev_read_at) AS prev_read_at_year,
        quality,
        PERCENTILE_APPROX(session_no, 0.5) AS median_session_no,
        SUM(CASE WHEN quality = 'EST' THEN 1 ELSE 0 END) AS n_quality_est
    FROM sessionized
    GROUP BY YEAR(prev_read_at), quality
    HAVING MAX(voltage_plus_kwh) > 109.5
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        prev_read_at_year,
        quality,
        median_session_no,
        n_quality_est,
        ROW_NUMBER() OVER (PARTITION BY quality ORDER BY median_session_no ASC, prev_read_at_year, quality) AS median_session_no_seq,
        COUNT(*) OVER (PARTITION BY n_quality_est) AS rows_in_n_quality_est,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY n_quality_est)) AS distinct_quality_in_n_quality_est
    FROM totals
)
-- Final projection
SELECT
    prev_read_at_year,
    quality,
    n_quality_est,
    median_session_no_seq,
    rows_in_n_quality_est
FROM windowed
WHERE distinct_quality_in_n_quality_est > 3
ORDER BY median_session_no_seq DESC NULLS FIRST
