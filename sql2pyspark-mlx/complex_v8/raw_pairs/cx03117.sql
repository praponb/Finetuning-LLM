WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE is_explicit = TRUE
),
with_activity AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM valid_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
        )
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) FILTER (WHERE device IN ('speaker', 'tv', 'car')) AS play_device_count
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_device_count, 0) AS play_device_count
    FROM with_activity s
    LEFT JOIN play_stats p ON p.track_id = s.id
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    release_date,
    play_device_count,
    DATE_TRUNC('WEEK', release_date) AS release_date_week,
    LENGTH(TRIM(title)) AS title_len
FROM summarized_rows
WHERE play_device_count >= 2
ORDER BY release_date DESC
