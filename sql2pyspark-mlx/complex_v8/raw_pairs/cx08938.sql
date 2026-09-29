WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref
    FROM tracks
    WHERE bpm >= 100
),
combined_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.title_slug,
        a.debut_year AS artist_debut_year
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.artist_debut_year,
        a.artist_name,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM combined_details_artist s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre IS NOT NULL
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) FILTER (WHERE source <> 'radio') AS play_source_count,
        COUNT(CASE WHEN source <> 'album' THEN 1 END) AS play_source_count_2,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count,
        COALESCE(p.play_source_count_2, 0) AS play_source_count_2,
        COALESCE(p.play_count, 0) AS play_count
    FROM combined_details s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(artist_genre)) AS artist_genre_len,
        UPPER(TRIM(artist_name)) AS artist_name_clean
    FROM summarized_rows
    WHERE release_date IS NOT NULL
),
combined_details_2 AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.artist_debut_year,
        s.artist_name,
        s.artist_genre,
        s.artist_is_verified,
        s.play_count,
        s.artist_genre_len,
        s.artist_name_clean,
        a.artist_name AS artist_artist_name,
        a.is_verified AS artist_is_verified_2,
        a.debut_year AS artist_debut_year_2
    FROM tagged s
    JOIN artists a ON a.id = s.artist_id
),
with_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.artist_debut_year,
        s.artist_name,
        s.play_count,
        s.artist_genre_len,
        s.artist_artist_name,
        s.artist_is_verified_2,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified_3,
        a.genre AS artist_genre_2
    FROM combined_details_2 s
    JOIN artists a ON a.id = s.artist_id
)
SELECT
    id,
    artist_id,
    release_date,
    artist_debut_year,
    artist_name,
    play_count,
    artist_genre_len,
    artist_artist_name,
    artist_is_verified_3,
    artist_genre_2
FROM with_details
ORDER BY release_date DESC, artist_genre_2 DESC
