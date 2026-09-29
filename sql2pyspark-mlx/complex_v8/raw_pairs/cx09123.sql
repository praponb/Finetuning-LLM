WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
    WHERE device = 'tv'
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM scoped_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        track_id,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(device_len) AS session_device_len,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY track_id, session_id
),
joined_track AS (
    -- Join tracks
    SELECT
        s.track_id,
        s.session_id,
        s.session_start,
        s.session_end,
        s.event_count,
        s.duration_min,
        t.artist_id AS track_artist_id,
        a.genre AS artist_genre
    FROM session_summary s
    LEFT JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM joined_track s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.track_artist_id
        )
)
SELECT
    track_id,
    session_start,
    session_end,
    event_count,
    duration_min,
    track_artist_id,
    artist_genre,
    session_end + INTERVAL 7 DAYS AS session_end_plus_7d,
    DATE_FORMAT(session_start, 'yyyy-MM') AS session_start_label
FROM without_matches
WHERE artist_genre = 'Indie'
ORDER BY track_artist_id DESC, session_start
