WITH prepared_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE bpm BETWEEN 89 AND 142
      AND release_date < DATE '2025-03-08'
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM prepared_tracks
    WHERE bpm < (SELECT MAX(bpm) FROM prepared_tracks)
),
exploded AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        t.mood
    FROM with_activity s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
filtered AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM exploded s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.release_date < DATE '2025-08-07'
              AND LENGTH(TRIM(t.title)) >= 6
        )
      AND s.mood = 'workout'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT artist_id, release_date, mood, duration_sec
    FROM filtered
) src
PIVOT (
    AVG(duration_sec) AS avg_v, MAX(duration_sec) AS max_v
    FOR mood IN ('chill', 'sad', 'workout')
)
ORDER BY artist_id, release_date
