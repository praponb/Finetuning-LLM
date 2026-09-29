WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        DATE_TRUNC('YEAR', release_date) AS release_date_year,
        CASE WHEN bpm >= 156 THEN 'A' WHEN bpm >= 96 THEN 'B' ELSE 'C' END AS bpm_tier,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE YEAR(release_date) = 2024
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(CASE WHEN is_skipped = FALSE THEN 1 END) AS play_is_skipped_count,
        MIN(played_at) AS first_played_at,
        COUNT(CASE WHEN NOT is_skipped THEN 1 END) AS play_is_skipped_count_2
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count,
        p.first_played_at,
        COALESCE(p.play_is_skipped_count_2, 0) AS play_is_skipped_count_2
    FROM scoped_tracks s
    LEFT JOIN play_stats p ON p.track_id = s.id
)
SELECT
    COALESCE(bpm_tier, 'ALL') AS bpm_tier,
    play_is_skipped_count,
    play_is_skipped_count_2,
    ROUND(AVG(play_is_skipped_count), 2) AS avg_play_is_skipped_count,
    COUNT(DISTINCT artist_id) AS distinct_artist_ids
FROM with_stats
GROUP BY GROUPING SETS ((bpm_tier, play_is_skipped_count, play_is_skipped_count_2), (bpm_tier), ())
