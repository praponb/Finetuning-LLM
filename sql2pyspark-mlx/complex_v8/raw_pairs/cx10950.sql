WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm
    FROM tracks
    WHERE bpm > 82
),
combined_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.artist_name,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre
    FROM scoped s
    JOIN artists a ON a.id = s.artist_id
)
SELECT id, metric, metric_value
FROM (
    SELECT id, bpm, duration_sec
    FROM combined_details_artist
) src
UNPIVOT (
    metric_value FOR metric IN (bpm, duration_sec)
) unp
