WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE bpm < 127
      AND artist_id IS NOT NULL
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        t.mood
    FROM valid s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT artist_id, mood, bpm
    FROM per_label
) src
PIVOT (
    MAX(bpm) AS max_v, SUM(bpm) AS sum_v
    FOR mood IN ('party', 'chill', 'sad')
)
