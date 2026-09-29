WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
latest AS (
    -- Keep the latest row per source, track_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        prev_played_at,
        new_session_flag,
        session_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source, track_id ORDER BY session_id DESC, id DESC) AS row_num
        FROM sessionized
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    is_skipped,
    source,
    prev_played_at,
    new_session_flag,
    ROUND(session_id / NULLIF(new_session_flag, 0), 3) AS session_id_per_new_session_flag,
    TO_DATE(prev_played_at) AS prev_played_at_date
FROM latest
ORDER BY listener_id
