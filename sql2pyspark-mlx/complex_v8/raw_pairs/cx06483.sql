WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage > 214.3
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        quality,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_no
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        duration_min - event_count AS duration_min_minus_event_count,
        ABS(duration_min - 120.0) AS duration_min_distance
    FROM sessions
    WHERE session_no >= 2
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        quality,
        session_no,
        session_start,
        session_end,
        session_kwh,
        duration_min_minus_event_count,
        duration_min_distance,
        MIN(session_kwh) OVER (PARTITION BY event_count ORDER BY duration_min_distance, quality, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_session_kwh_so_far,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        ROUND(duration_min / NULLIF(SUM(duration_min) OVER (PARTITION BY quality), 0), 4) AS duration_min_share_of_quality,
        ROUND(AVG(duration_min_distance) OVER (PARTITION BY event_count ORDER BY duration_min_distance, quality, session_no ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS duration_min_distance_avg_4,
        ROUND(AVG(duration_min_distance) OVER (PARTITION BY event_count ORDER BY duration_min_distance, quality, session_no ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS duration_min_distance_avg_6_prior,
        ROUND(session_kwh - AVG(session_kwh) OVER (PARTITION BY quality), 2) AS session_kwh_vs_quality_avg
    FROM annotated
)
-- Native SQL UNPIVOT (wide to long)
SELECT quality, session_no, field, amount
FROM (
    SELECT quality, session_no, session_kwh_vs_quality_avg, session_kwh, min_session_kwh_so_far
    FROM sequenced
) src
UNPIVOT (
    amount FOR field IN (session_kwh_vs_quality_avg AS m_session_kwh_vs_quality_avg, session_kwh AS session_kwh_v, min_session_kwh_so_far AS min_session_kwh_so_far_v)
) unp
ORDER BY quality, session_no, field
