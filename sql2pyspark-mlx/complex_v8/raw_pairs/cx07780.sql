WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit
    FROM tracks
    WHERE bpm >= 89
      AND duration_sec <= 344
),
enriched AS (
    -- Derive helper columns
    SELECT *, DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date
    FROM valid_tracks
    WHERE artist_id IS NOT NULL
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at
    FROM enriched s
    LEFT JOIN play_stats p ON p.track_id = s.id
)
SELECT
    id,
    title,
    release_date,
    is_explicit,
    days_since_release_date,
    first_played_at,
    CASE WHEN days_since_release_date >= 560 THEN 'large' WHEN days_since_release_date >= 175 THEN 'medium' ELSE 'small' END AS days_since_release_date_tier
FROM summarized_rows
ORDER BY days_since_release_date DESC
