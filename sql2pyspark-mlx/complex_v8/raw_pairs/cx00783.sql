WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        QUARTER(release_date) AS release_date_quarter,
        duration_sec - bpm AS duration_sec_minus_bpm
    FROM tracks
),
filtered AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM recent_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.is_explicit = FALSE
        )
      AND s.bpm BETWEEN 82 AND 165
)
SELECT
    artist_id,
    is_explicit,
    release_date_quarter,
    SUM(bpm) AS total_bpm
FROM filtered
GROUP BY ROLLUP(artist_id, is_explicit, release_date_quarter)
ORDER BY artist_id NULLS LAST, is_explicit NULLS LAST, release_date_quarter NULLS LAST
