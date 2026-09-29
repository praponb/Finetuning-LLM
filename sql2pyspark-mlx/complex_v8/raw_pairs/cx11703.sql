WITH filtered AS (
    -- Filter plays
    SELECT *
    FROM plays
    WHERE track_id <= 2
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_parent AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.prev_played_at,
        t.release_date AS track_release_date,
        l.interests AS listener_interests
    FROM sessionized s
    JOIN tracks t ON t.id = s.track_id AND t.bpm < 156
    LEFT JOIN listeners l ON l.id = s.listener_id
)
-- Final projection
SELECT
    id,
    track_id,
    played_at,
    ms_played,
    source,
    listener_interests,
    CONCAT(source, '#', CAST(ms_played AS STRING)) AS source_ref
FROM with_parent
ORDER BY ms_played DESC NULLS FIRST
