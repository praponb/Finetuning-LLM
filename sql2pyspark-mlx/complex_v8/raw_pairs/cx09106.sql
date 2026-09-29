WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source
    FROM plays
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM scoped_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM sessionized s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
        )
      AND MONTH(s.played_at) IN (2, 3)
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.listener_id = s.listener_id) AS max_play_ms_played,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at
    FROM filtered s
)
SELECT *
FROM (
    SELECT track_id, source, ms_played
    FROM profiled
) src
PIVOT (
    MAX(ms_played)
    FOR source IN ('search', 'radio', 'album', 'playlist')
)
