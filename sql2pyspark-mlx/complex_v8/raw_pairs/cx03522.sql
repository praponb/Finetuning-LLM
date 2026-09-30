WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix,
        COALESCE(bpm, 0) AS bpm_filled,
        DAYOFWEEK(release_date) AS release_date_dayofweek
    FROM tracks
    WHERE is_explicit = TRUE
),
joined AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.moods,
        s.title_prefix,
        s.bpm_filled,
        a.artist_name
    FROM cleaned s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.artist_name IS NOT NULL
),
with_activity AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
        )
),
combined_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.bpm_filled,
        s.artist_name,
        a.artist_name AS artist_artist_name
    FROM with_activity s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    id,
    bpm_filled,
    artist_name,
    CONCAT_WS(' / ', artist_artist_name, artist_name) AS artist_artist_name_artist_name_label
FROM combined_details
