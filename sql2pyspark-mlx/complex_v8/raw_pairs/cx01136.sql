WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        ADD_MONTHS(release_date, 6) AS release_date_plus_6m,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND SIZE(moods) >= 2
),
qualified AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM valid_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.genre = 'Jazz'
              AND a.debut_year >= 1996
        )
)
SELECT
    is_explicit,
    artist_id,
    MAX(duration_sec_filled) AS max_duration_sec_filled
FROM qualified
GROUP BY ROLLUP(is_explicit, artist_id)
