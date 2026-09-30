WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        moods,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix
    FROM tracks
),
labeled AS (
    -- Add calculated columns
    SELECT *, IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM scoped_tracks
),
enriched AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.moods,
        s.title_prefix,
        s.is_explicit_label,
        a.is_verified AS artist_is_verified
    FROM labeled s
    JOIN artists a ON a.id = s.artist_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(title_prefix, '#', CAST(artist_id AS STRING)) AS title_prefix_ref
    FROM enriched
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.artist_name,
        a.genre AS artist_genre,
        a.country AS artist_country
    FROM tagged s
    JOIN artists a ON a.id = s.artist_id
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_parent_artist s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.device NOT IN ('phone', 'desktop')
              AND p.source = 'playlist'
        )
),
joined AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.moods,
        s.title_prefix,
        s.is_explicit_label,
        s.artist_is_verified,
        s.artist_name,
        s.artist_genre,
        s.artist_country,
        a.is_verified AS artist_is_verified_2
    FROM with_activity s
    LEFT JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    is_explicit,
    moods,
    is_explicit_label,
    artist_is_verified,
    artist_genre,
    artist_is_verified_2,
    CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
FROM joined
ORDER BY artist_genre NULLS LAST
