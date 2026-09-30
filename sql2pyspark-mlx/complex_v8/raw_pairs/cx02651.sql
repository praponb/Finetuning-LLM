WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM base_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        quality,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        quality,
        session_id,
        session_start,
        session_kwh,
        duration_min,
        COUNT(*) OVER w_session_id AS rows_in_session_id,
        SUM(session_kwh) OVER (PARTITION BY quality, session_id ORDER BY duration_min DESC, quality, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_session_kwh,
        FIRST_VALUE(session_id) OVER (PARTITION BY quality, session_id ORDER BY duration_min DESC, quality, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_session_id,
        MIN(event_count) OVER (PARTITION BY session_id) AS session_id_min_event_count,
        ROUND(duration_min - AVG(duration_min) OVER (PARTITION BY session_id), 2) AS duration_min_vs_session_id_avg,
        RANK() OVER (PARTITION BY quality, session_id ORDER BY duration_min DESC) AS duration_min_rank
    FROM sessions
    WINDOW w_session_id AS (PARTITION BY session_id)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM ranked
    WHERE duration_min_rank < (SELECT MAX(duration_min_rank) FROM ranked)
)
-- Final projection
SELECT
    quality,
    session_kwh,
    duration_min,
    rows_in_session_id,
    running_session_kwh,
    duration_min_rank,
    CONCAT(quality, '#', CAST(rows_in_session_id AS STRING)) AS quality_ref,
    ROUND(session_kwh / NULLIF(session_id, 0), 3) AS session_kwh_per_session_id
FROM qualified
