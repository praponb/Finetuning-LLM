WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        QUARTER(release_date) AS release_date_quarter
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'workout')
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.track_id = s.id) AS avg_play_ms_played,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.track_id = s.id) AS avg_play_ms_played_2,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at
    FROM cleaned s
),
grouped AS (
    -- Group-level totals
    SELECT
        artist_id,
        CASE WHEN avg_play_ms_played_2 >= 242000.0 THEN 'large' WHEN avg_play_ms_played_2 >= 163000.0 THEN 'medium' ELSE 'small' END AS avg_play_ms_played_2_tier,
        DATE_TRUNC('MONTH', last_played_at) AS last_played_at_month,
        COUNT(*) AS cnt,
        ROUND(100 * COUNT(CASE WHEN title IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_title_is_not_null,
        ROUND(SUM(avg_play_ms_played) / NULLIF(SUM(bpm), 0), 3) AS avg_play_ms_played_to_bpm_ratio,
        SUM(CASE WHEN is_explicit = TRUE THEN duration_sec ELSE 0 END) AS duration_sec_is_explicit_true,
        MAX(last_played_at) AS last_last_played_at,
        ROUND(SUM(bpm) / NULLIF(SUM(avg_play_ms_played_2), 0), 3) AS bpm_to_avg_play_ms_played_2_ratio
    FROM with_counts
    WHERE avg_play_ms_played > 143250.0
    GROUP BY 1, 2, 3
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count
    FROM grouped s
)
-- Final projection
SELECT
    artist_id,
    avg_play_ms_played_2_tier,
    avg_play_ms_played_to_bpm_ratio,
    duration_sec_is_explicit_true,
    last_last_played_at
FROM with_stats
WHERE last_last_played_at >= TIMESTAMP '2025-01-13 09:00:00'
ORDER BY artist_id NULLS LAST
