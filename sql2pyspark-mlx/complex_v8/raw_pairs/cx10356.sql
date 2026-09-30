WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 308104 THEN 'HIGH' WHEN ms_played >= 151112 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier
    FROM plays
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM recent_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.source,
        s.ms_played_tier,
        s.prev_played_at,
        s.is_new_session,
        s.session_no,
        t.title AS track_title
    FROM sessionized s
    LEFT JOIN tracks t ON t.id = s.track_id
)
SELECT
    id,
    listener_id,
    track_id,
    source,
    ms_played_tier,
    prev_played_at,
    session_no,
    track_title,
    LENGTH(TRIM(ms_played_tier)) AS ms_played_tier_len,
    COALESCE(source, 'UNKNOWN') AS source_or_unknown
FROM joined
WHERE is_new_session > 0
