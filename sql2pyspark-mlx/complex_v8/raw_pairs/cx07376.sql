WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE SIZE(moods) >= 2
      AND bpm >= 76
),
artist_id_stats AS (
    -- Avg bpm per artist_id
    SELECT artist_id, AVG(bpm) AS avg_bpm
    FROM filtered
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_bpm, ROUND(s.bpm - g.avg_bpm, 2) AS bpm_minus_avg
    FROM filtered s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm > g.avg_bpm
),
filtered_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM vs_group
    WHERE bpm_minus_avg > (SELECT MIN(bpm_minus_avg) FROM vs_group)
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.moods,
        s.avg_bpm,
        p.id AS play_id,
        p.device AS play_device,
        p.source AS play_source,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped
    FROM filtered_2 s
    LEFT JOIN plays p ON p.track_id = s.id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        play_id,
        SUM(avg_bpm) FILTER (WHERE is_explicit = TRUE) AS avg_bpm_is_explicit_true,
        COUNT(DISTINCT play_source) AS distinct_play_sources
    FROM with_children_plays
    GROUP BY artist_id, play_id
    HAVING SUM(play_ms_played) > 108979
)
-- Final projection
SELECT
    artist_id,
    play_id,
    avg_bpm_is_explicit_true,
    CASE WHEN avg_bpm_is_explicit_true >= 585.0 THEN 'large' WHEN avg_bpm_is_explicit_true >= 360.0 THEN 'medium' ELSE 'small' END AS avg_bpm_is_explicit_true_tier
FROM per_group
ORDER BY avg_bpm_is_explicit_true_tier NULLS LAST
