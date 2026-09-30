WITH filtered_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag,
        UPPER(TRIM(country)) AS country_clean
    FROM artists
    WHERE country IS NOT NULL
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM filtered_artists
    WHERE is_verified_flag > (SELECT MIN(is_verified_flag) FROM filtered_artists)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(genre), 1, 2) AS genre_prefix,
        CASE WHEN is_verified_flag >= 1 THEN 'HIGH' WHEN is_verified_flag >= 0 THEN 'MEDIUM' ELSE 'LOW' END AS is_verified_flag_tier
    FROM screened
    WHERE genre NOT IN ('Jazz', 'Classical')
),
exploded_detail_tracks AS (
    -- One row per track
    SELECT
        s.id,
        s.artist_name,
        s.country,
        s.debut_year,
        s.is_verified_flag,
        t.id AS track_id,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title,
        t.bpm AS track_bpm
    FROM tagged s
    LEFT JOIN tracks t ON t.artist_id = s.id
)
SELECT *
FROM (
    SELECT is_verified_flag, country, track_duration_sec
    FROM exploded_detail_tracks
) src
PIVOT (
    SUM(track_duration_sec)
    FOR country IN ('US', 'TH', 'JP')
)
ORDER BY is_verified_flag
