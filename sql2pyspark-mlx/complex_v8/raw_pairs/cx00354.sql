WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        LAST_DAY(release_date) AS release_date_month_end
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'S%'
      AND release_date BETWEEN DATE '2024-04-21' AND DATE '2025-11-29'
),
joined AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.moods,
        s.release_date_month_end,
        a.artist_name
    FROM recent s
    LEFT JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    id,
    artist_id,
    moods,
    release_date_month_end,
    artist_name
FROM joined
WHERE artist_id IS NOT NULL
