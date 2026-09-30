WITH valid AS (
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
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        site_id,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(session_id - AVG(session_id) OVER (PARTITION BY session_id), 2) AS session_id_vs_session_id_avg,
        RANK() OVER (PARTITION BY site_id, n_events ORDER BY session_end DESC, site_id, session_id) AS session_end_rank,
        MAX(n_events) OVER (PARTITION BY session_id, n_events ORDER BY session_end DESC, site_id, session_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS n_events_max_2
    FROM sessions
)
SELECT
    site_id,
    session_id,
    session_start,
    session_end,
    n_events,
    duration_min,
    session_end_rank
FROM with_trends
ORDER BY site_id DESC, session_id
LIMIT 20
