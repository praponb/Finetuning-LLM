WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label
    FROM tracks
    WHERE (bpm <= 163 OR title IS NOT NULL)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.country AS artist_country,
        a.artist_name,
        a.genre AS artist_genre
    FROM cleaned s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.country = 'BR'
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        CONCAT_WS(' / ', artist_genre, release_date_label) AS artist_genre_release_date_label_label
    FROM combined_details
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.artist_country,
        s.artist_genre,
        s.is_explicit_label,
        s.artist_genre_release_date_label_label,
        a.debut_year AS artist_debut_year,
        a.artist_name AS artist_artist_name
    FROM derived s
    JOIN artists a ON a.id = s.artist_id AND a.debut_year > 1997
)
-- Final projection
SELECT
    id,
    artist_id,
    is_explicit,
    artist_country,
    artist_genre,
    is_explicit_label,
    artist_genre_release_date_label_label,
    artist_artist_name,
    CASE WHEN artist_genre IN ('Indie', 'HipHop') THEN 'group_a' WHEN artist_genre = 'Classical' THEN 'group_b' ELSE 'other' END AS artist_genre_group,
    COALESCE(artist_genre, 'UNKNOWN') AS artist_genre_or_unknown
FROM with_details_artist
ORDER BY artist_genre_or_unknown DESC, id NULLS LAST
