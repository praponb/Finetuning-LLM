WITH valid AS (
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
        SUBSTRING(TRIM(source), 1, 2) AS source_prefix
    FROM plays
    WHERE ms_played <= 214343
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_parent_track AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.is_explicit AS track_is_explicit,
        t.artist_id AS track_artist_id,
        t.duration_sec AS track_duration_sec,
        l.interests AS listener_interests
    FROM sessionized s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
),
grouped AS (
    -- Summarise per group
    SELECT
        track_id,
        ROUND(AVG(track_duration_sec), 2) AS avg_track_duration_sec,
        SUM(CASE WHEN is_skipped = TRUE THEN 1 ELSE 0 END) AS n_is_skipped_true,
        MAX(prev_played_at) AS last_prev_played_at,
        SUM(track_duration_sec) FILTER (WHERE is_skipped = FALSE) AS track_duration_sec_is_skipped_false
    FROM with_parent_track
    GROUP BY track_id
)
-- Final projection
SELECT
    track_id,
    avg_track_duration_sec,
    n_is_skipped_true,
    last_prev_played_at,
    track_duration_sec_is_skipped_false
FROM grouped
ORDER BY track_duration_sec_is_skipped_false DESC, avg_track_duration_sec DESC NULLS FIRST
