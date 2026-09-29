WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at + INTERVAL 15 MINUTES AS read_at_plus_15m
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        quality,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_no
),
derived AS (
    -- Label and score rows
    SELECT *, DATE_TRUNC('QUARTER', session_start) AS session_start_quarter
    FROM session_summary
),
stats AS (
    -- Summarise per group
    SELECT
        YEAR(session_start) AS session_start_year,
        DATE_TRUNC('MONTH', session_start_quarter) AS session_start_quarter_month,
        quality,
        ROUND(SUM(session_kwh) / NULLIF(SUM(session_no), 0), 3) AS session_kwh_to_session_no_ratio,
        ROUND(SUM(duration_min), 2) AS total_duration_min,
        COUNT(*) FILTER (WHERE quality NOT IN ('OK', 'BAD')) AS n_quality_not_in_ok_bad,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE quality = 'BAD') AS n_quality_bad
    FROM derived
    GROUP BY YEAR(session_start), DATE_TRUNC('MONTH', session_start_quarter), quality
)
SELECT session_start_year, session_kwh_to_session_no_ratio, cnt
FROM stats
ORDER BY session_start_year DESC
