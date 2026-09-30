WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM tracks
    WHERE (UPPER(TRIM(title)) LIKE 'M%' OR is_explicit = FALSE)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref,
        CONCAT_WS('|', title_prefix, title) AS title_prefix_title_label
    FROM filtered_tracks
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.moods,
        s.title_ref,
        a.artist_name
    FROM tagged s
    JOIN artists a ON a.id = s.artist_id
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        moods,
        artist_name,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text
    FROM combined_details_artist
),
without_matches AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.genre <> 'Pop'
              AND a.is_verified = TRUE
        )
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    moods
FROM without_matches
ORDER BY artist_id NULLS LAST, id
