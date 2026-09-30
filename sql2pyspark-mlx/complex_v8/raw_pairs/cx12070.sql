WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        MONTH(release_date) AS release_date_month
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'chill')
      AND duration_sec >= 142
),
joined AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.release_date_month,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified,
        a.artist_name
    FROM valid s
    JOIN artists a ON a.id = s.artist_id
)
SELECT *
FROM (
    SELECT is_explicit, artist_id, artist_country, release_date_month
    FROM joined
) src
PIVOT (
    AVG(release_date_month)
    FOR artist_country IN ('JP', 'US', 'KR', 'BR')
)
