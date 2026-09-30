WITH scoped AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE duration_sec > 209
      AND artist_id IS NOT NULL
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.title_ref,
        s.title_slug,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country,
        a.genre AS artist_genre
    FROM scoped s
    JOIN artists a ON a.id = s.artist_id
),
enriched_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.title_ref,
        s.artist_country,
        s.artist_genre,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre_2
    FROM enriched s
    JOIN artists a ON a.id = s.artist_id
),
deduped AS (
    -- Keep the earliest row per artist_country, artist_genre
    SELECT
        id,
        artist_id,
        title,
        bpm,
        title_ref,
        artist_country,
        artist_genre,
        artist_debut_year,
        artist_genre_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_country, artist_genre ORDER BY bpm, id) AS row_num
        FROM enriched_artist
    ) ranked
    WHERE row_num = 1
),
unique_rows AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        bpm,
        title_ref,
        artist_country,
        artist_genre,
        artist_debut_year,
        artist_genre_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm, id) AS recency_rank
        FROM deduped
    ) ranked
    WHERE recency_rank = 1
),
screened AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM unique_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.country IS NOT NULL
        )
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.title_ref,
        s.artist_genre,
        s.artist_genre_2,
        a.debut_year AS artist_debut_year_2
    FROM screened s
    JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    bpm,
    title_ref,
    artist_genre,
    artist_genre_2,
    artist_debut_year_2,
    CASE WHEN artist_genre IN ('HipHop', 'Classical') THEN 'group_a' WHEN artist_genre = 'Pop' THEN 'group_b' ELSE 'other' END AS artist_genre_group
FROM with_details_artist
ORDER BY title DESC
