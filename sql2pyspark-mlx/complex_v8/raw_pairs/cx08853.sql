WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
    WHERE played_at < TIMESTAMP '2025-03-02 09:00:00'
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source,
        s.days_since_played_at,
        s.prev_played_at,
        s.new_session_flag,
        s.session_id,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped
    FROM sessionized s
    JOIN plays p ON p.track_id = s.track_id
)
SELECT *
FROM (
    SELECT play_id, device, days_since_played_at
    FROM line_level
) src
PIVOT (
    MAX(days_since_played_at) AS max_v, COUNT(days_since_played_at) AS n
    FOR device IN ('tv', 'car', 'phone', 'speaker')
)
ORDER BY play_id
