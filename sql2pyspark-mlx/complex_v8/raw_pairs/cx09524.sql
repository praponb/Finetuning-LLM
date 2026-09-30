WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE duration_sec < 365
      AND LOWER(title) LIKE '%on%'
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        title_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id DESC) AS recency_rank
        FROM base_tracks
    ) ranked
    WHERE recency_rank = 1
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    bpm,
    moods,
    title_prefix,
    CASE WHEN duration_sec >= 352 THEN 'large' WHEN duration_sec >= 213 THEN 'medium' ELSE 'small' END AS duration_sec_tier
FROM without_matches
ORDER BY duration_sec_tier DESC
