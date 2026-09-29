WITH recent_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'workout')
      AND release_date IS NOT NULL
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.moods_text,
        a.genre AS artist_genre
    FROM recent_tracks s
    JOIN artists a ON a.id = s.artist_id AND a.genre NOT IN ('Indie', 'Pop')
    WHERE a.genre IN ('Jazz', 'Pop')
),
scored AS (
    -- Derive helper columns
    SELECT *, FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM with_parent_artist
    WHERE duration_sec < 409
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(duration_sec_bucket, duration_sec) AS least_duration_sec_bucket_duration_sec,
        LOWER(TRIM(artist_genre)) AS artist_genre_clean,
        COALESCE(artist_genre, 'UNKNOWN') AS artist_genre_or_unknown
    FROM scored
    WHERE artist_id <= 10
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.moods_text,
        s.duration_sec_bucket,
        s.least_duration_sec_bucket_duration_sec,
        s.artist_genre_or_unknown,
        a.genre AS artist_genre_2
    FROM labeled s
    LEFT JOIN artists a ON a.id = s.artist_id
),
combined_details_artist_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.moods_text,
        s.artist_genre_2,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM combined_details_artist s
    JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    id,
    duration_sec,
    moods_text,
    artist_genre_2,
    artist_genre,
    LENGTH(TRIM(moods_text)) AS moods_text_len
FROM combined_details_artist_2
WHERE artist_is_verified = TRUE
ORDER BY artist_genre_2 DESC
