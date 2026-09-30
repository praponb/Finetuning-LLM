WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        LOWER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'sad')
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at
    FROM prepared s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        days_since_release_date,
        title_clean,
        first_played_at,
        LOWER(REGEXP_REPLACE(TRIM(title_clean), ' ', '_')) AS title_clean_slug,
        DATEDIFF(DATE '2025-06-30', first_played_at) AS days_since_first_played_at
    FROM profiled
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(first_played_at, 2) OVER (PARTITION BY artist_id ORDER BY first_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_played_at,
        MAX(days_since_first_played_at) OVER (PARTITION BY artist_id) AS artist_id_max_days_since_first_played_at,
        ROUND(AVG(days_since_first_played_at) OVER (PARTITION BY artist_id ORDER BY first_played_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS days_since_first_played_at_avg_6_prior,
        ROUND(days_since_first_played_at / NULLIF(SUM(days_since_first_played_at) OVER (PARTITION BY artist_id), 0), 4) AS days_since_first_played_at_share_of_artist_id
    FROM annotated
)
SELECT
    id,
    artist_id,
    days_since_release_date,
    first_played_at,
    title_clean_slug,
    days_since_first_played_at,
    second_first_played_at,
    artist_id_max_days_since_first_played_at,
    days_since_first_played_at_avg_6_prior,
    CASE WHEN days_since_release_date >= 595 THEN 'A' WHEN days_since_release_date >= 210 THEN 'B' ELSE 'C' END AS days_since_release_date_tier
FROM with_trends
ORDER BY second_first_played_at DESC, days_since_first_played_at DESC
