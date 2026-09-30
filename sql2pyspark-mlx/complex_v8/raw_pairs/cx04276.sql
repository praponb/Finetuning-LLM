WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        HOUR(played_at) AS played_at_hour
    FROM plays
    WHERE is_skipped = FALSE
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        ROUND(played_at_hour / NULLIF(ms_played, 0), 3) AS played_at_hour_per_ms_played,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM prepared
    WHERE played_at_hour <= 14
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(played_at_hour, 0) AS played_at_hour_filled,
        FLOOR(ms_played_filled / 50000) * 50000 AS ms_played_filled_bucket,
        CASE WHEN ms_played >= 290754 THEN 'large' WHEN ms_played >= 130888 THEN 'medium' ELSE 'small' END AS ms_played_tier
    FROM scored
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.listener_id = s.listener_id) AS total_play_ms_played,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count
    FROM derived s
),
with_parent_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.source_slug,
        s.played_at_hour_per_ms_played,
        s.ms_played_filled,
        s.played_at_hour_filled,
        s.ms_played_filled_bucket,
        s.ms_played_tier,
        s.total_play_ms_played,
        s.play_count,
        l.country AS listener_country,
        l.username AS listener_username,
        l.plan AS listener_plan
    FROM summarized_rows s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
device_stats AS (
    -- Median play_count per device
    SELECT device, PERCENTILE_APPROX(play_count, 0.5) AS median_play_count
    FROM with_parent_listener
    GROUP BY device
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_play_count,
        ROUND(s.play_count - g.median_play_count, 2) AS play_count_minus_median
    FROM with_parent_listener s
    JOIN device_stats g ON g.device = s.device
    WHERE s.play_count >= g.median_play_count
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    is_skipped,
    source,
    ms_played_filled,
    played_at_hour_filled,
    ms_played_filled_bucket,
    total_play_ms_played,
    listener_country,
    listener_username,
    listener_plan,
    median_play_count,
    CASE WHEN source IN ('search', 'radio') THEN 'group_a' WHEN source = 'playlist' THEN 'group_b' ELSE 'other' END AS source_group,
    GREATEST(ms_played_filled, ms_played_filled_bucket) AS greatest_ms_played_filled_ms_played_filled_bucke
FROM above_average
ORDER BY id, total_play_ms_played DESC NULLS FIRST
LIMIT 10
