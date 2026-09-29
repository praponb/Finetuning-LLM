WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        duration_sec + bpm AS duration_sec_plus_bpm
    FROM tracks
    WHERE SIZE(moods) >= 1
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT s.*, a.is_verified AS artist_is_verified
    FROM recent s
    JOIN artists a ON a.id = s.artist_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.avg_play_ms_played,
        COALESCE(p.play_count, 0) AS play_count
    FROM joined_artist s
    LEFT JOIN play_stats p ON p.track_id = s.id
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    moods,
    duration_sec_per_bpm,
    duration_sec_plus_bpm,
    artist_is_verified,
    avg_play_ms_played,
    play_count,
    ARRAY_CONTAINS(moods, 'focus') AS has_focus,
    LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
FROM summarized_rows
WHERE days_since_release_date >= 315
