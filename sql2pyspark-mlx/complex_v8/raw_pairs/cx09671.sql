WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        moods
    FROM tracks
    WHERE NOT is_explicit
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        t.mood
    FROM prepared s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        mood,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_mood_artist_id, 0), 4) AS duration_sec_share_of_mood_artist_id,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY artist_id, mood ORDER BY release_date DESC, id, mood)) AS days_since_prev_release_date,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY mood, artist_id), 2) AS duration_sec_vs_mood_artist_id_avg
    FROM flattened
    WINDOW w_mood_artist_id AS (PARTITION BY mood, artist_id)
)
-- Final projection
SELECT
    id,
    artist_id,
    mood,
    days_since_prev_release_date,
    duration_sec_vs_mood_artist_id_avg,
    CONCAT(mood, '#', CAST(artist_id AS STRING)) AS mood_ref
FROM sequenced
WHERE mood IN ('chill', 'focus')
ORDER BY mood NULLS LAST
