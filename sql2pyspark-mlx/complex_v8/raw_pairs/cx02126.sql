WITH prepared_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source
    FROM plays
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM prepared_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
listener_id_stats AS (
    -- Avg starts_session per listener_id
    SELECT listener_id, AVG(starts_session) AS avg_starts_session
    FROM sessionized
    GROUP BY listener_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_starts_session,
        ROUND(s.starts_session - g.avg_starts_session, 2) AS starts_session_minus_avg
    FROM sessionized s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.starts_session > g.avg_starts_session
)
-- Final projection
SELECT
    id,
    track_id,
    played_at,
    is_skipped,
    source,
    prev_played_at,
    starts_session,
    session_no,
    avg_starts_session,
    starts_session_minus_avg
FROM vs_group
ORDER BY source DESC, track_id DESC
