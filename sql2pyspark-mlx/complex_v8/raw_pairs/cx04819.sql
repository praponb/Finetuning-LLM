WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        ABS(duration_sec - 183) AS duration_sec_distance,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date
    FROM tracks
    WHERE artist_id <= 10
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        duration_sec,
        days_since_release_date,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date_2
    FROM valid
    WHERE days_since_release_date <= 490
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) FILTER (WHERE source IN ('radio', 'playlist')) AS play_source_count,
        COUNT(*) AS play_count,
        MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count,
        COALESCE(p.play_count, 0) AS play_count,
        p.first_played_at
    FROM enriched s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        YEAR(first_played_at) AS first_played_at_year,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        TO_DATE(first_played_at) AS first_played_at_date
    FROM summarized_rows
)
-- Final projection
SELECT
    id,
    artist_id,
    days_since_release_date,
    days_since_release_date_2,
    play_source_count,
    play_count,
    first_played_at_year,
    duration_sec_bucket,
    first_played_at_date,
    ADD_MONTHS(first_played_at_date, 1) AS first_played_at_date_plus_1m,
    duration_sec - first_played_at_year AS duration_sec_minus_first_played_at_year
FROM derived
