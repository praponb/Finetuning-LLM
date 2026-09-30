WITH filtered AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        LOWER(REGEXP_REPLACE(TRIM(artist_name), ' ', '_')) AS artist_name_slug
    FROM artists
    WHERE LENGTH(TRIM(artist_name)) >= 5
),
exploded_detail_tracks AS (
    -- One row per track
    SELECT
        s.id,
        s.genre,
        s.country,
        s.artist_name_slug,
        t.id AS track_id,
        t.moods AS track_moods,
        t.duration_sec AS track_duration_sec
    FROM filtered s
    LEFT JOIN tracks t ON t.artist_id = s.id AND t.duration_sec > 144
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        track_id,
        genre,
        country,
        ARRAY_SORT(COLLECT_SET(country)) AS country_list,
        MIN(track_duration_sec) AS min_track_duration_sec,
        ARRAY_SORT(COLLECT_SET(genre)) AS genre_list
    FROM exploded_detail_tracks
    GROUP BY track_id, genre, country
    HAVING AVG(track_duration_sec) > 183
)
SELECT
    track_id,
    country,
    country_list,
    min_track_duration_sec,
    genre_list,
    SUBSTRING(TRIM(genre), 1, 4) AS genre_prefix,
    ARRAY_JOIN(ARRAY_SORT(country_list), ',') AS country_list_text
FROM totals
WHERE track_id IS NOT NULL
ORDER BY country_list_text DESC
