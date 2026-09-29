WITH cleaned_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND NOT is_explicit
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        COUNT(*) AS row_count,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids
    FROM cleaned_tracks
    GROUP BY artist_id, DATE_TRUNC('MONTH', release_date)
),
joined_artist AS (
    -- Join artists
    SELECT
        s.artist_id,
        s.release_date_month,
        s.row_count,
        s.distinct_artist_ids,
        a.artist_name
    FROM grouped s
    LEFT JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    artist_id,
    release_date_month,
    row_count,
    distinct_artist_ids,
    artist_name
FROM joined_artist
ORDER BY release_date_month DESC, distinct_artist_ids
