WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        genre,
        debut_year,
        is_verified
    FROM artists
    WHERE debut_year > 1997
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM recent
    WHERE debut_year > (SELECT AVG(debut_year) FROM recent)
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MAX(release_date) AS last_track_release_date,
        MIN(release_date) AS first_track_release_date,
        ROUND(AVG(duration_sec), 2) AS avg_track_duration_sec
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.last_track_release_date,
        t.first_track_release_date,
        t.avg_track_duration_sec
    FROM qualified s
    LEFT JOIN track_stats t ON t.artist_id = s.id
),
tagged AS (
    -- Label and score rows
    SELECT *, ABS(debut_year - 2015) AS debut_year_distance
    FROM with_counts
    WHERE debut_year >= 1991
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT first_track_release_date, genre, avg_track_duration_sec
    FROM tagged
) src
PIVOT (
    MIN(avg_track_duration_sec)
    FOR genre IN ('Classical', 'Pop', 'Indie')
)
ORDER BY first_track_release_date
