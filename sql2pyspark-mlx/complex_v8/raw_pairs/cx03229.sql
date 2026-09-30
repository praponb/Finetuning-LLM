WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        CONCAT_WS(' / ', device, source) AS device_source_label,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
    WHERE (played_at >= TIMESTAMP '2025-01-31 10:00:00' OR is_skipped = TRUE)
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count
    FROM filtered_plays s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
play_stats_2 AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) FILTER (WHERE source NOT IN ('radio', 'album')) AS play_source_count
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count
    FROM with_counts s
    LEFT JOIN play_stats_2 p ON p.track_id = s.track_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        device_source_label,
        days_since_played_at,
        play_source_count,
        LOWER(REGEXP_REPLACE(TRIM(device_source_label), ' ', '_')) AS device_source_label_slug
    FROM with_stats
    WHERE days_since_played_at <= 630
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(ms_played) OVER (PARTITION BY is_skipped) AS is_skipped_sum_ms_played,
        ROUND(AVG(play_source_count) OVER (PARTITION BY is_skipped), 2) AS is_skipped_avg_play_source_count,
        MIN(days_since_played_at) OVER w_is_skipped AS is_skipped_min_days_since_played_at,
        LEAD(played_at) OVER (PARTITION BY MONTH(played_at) ORDER BY play_source_count NULLS LAST, id) AS next_played_at,
        ms_played - LAG(ms_played) OVER w_all_by_ms_played AS ms_played_change
    FROM enriched
    WINDOW w_is_skipped AS (PARTITION BY is_skipped), w_all_by_ms_played AS (ORDER BY ms_played ASC, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    is_skipped,
    source,
    device_source_label,
    days_since_played_at,
    device_source_label_slug,
    is_skipped_sum_ms_played,
    is_skipped_avg_play_source_count,
    is_skipped_min_days_since_played_at,
    COALESCE(ms_played, 0) AS ms_played_filled,
    DATE_TRUNC('MONTH', next_played_at) AS next_played_at_month
FROM metrics
ORDER BY ms_played_filled NULLS LAST, is_skipped_avg_play_source_count, id
LIMIT 5
