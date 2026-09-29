WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm
    FROM tracks
    WHERE release_date IS NOT NULL
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        a.genre AS artist_genre
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id AND LENGTH(TRIM(a.artist_name)) >= 5
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        title,
        artist_genre,
        CONCAT(artist_genre, '#', CAST(id AS STRING)) AS artist_genre_ref,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref,
        CASE WHEN artist_genre IN ('Indie', 'Jazz') THEN 'group_a' WHEN artist_genre = 'Pop' THEN 'group_b' ELSE 'other' END AS artist_genre_group
    FROM enriched_artist
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    artist_genre,
    artist_genre_group,
    INITCAP(TRIM(title_ref)) AS title_ref_clean,
    LOWER(REGEXP_REPLACE(TRIM(artist_genre), ' ', '_')) AS artist_genre_slug
FROM scored
WHERE title IS NOT NULL
ORDER BY title_ref_clean DESC, title DESC
