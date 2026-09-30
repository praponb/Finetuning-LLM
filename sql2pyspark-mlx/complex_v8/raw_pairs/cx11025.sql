WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        CASE WHEN ms_played >= 290754 THEN 'A' WHEN ms_played >= 95960 THEN 'B' ELSE 'C' END AS ms_played_tier
    FROM plays
    WHERE listener_id IS NOT NULL
      AND source IN ('radio', 'playlist')
),
combined_details AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.ms_played_bucket,
        s.is_skipped_flag,
        s.ms_played_tier,
        l.plan AS listener_plan,
        l.interests AS listener_interests,
        t.is_explicit AS track_is_explicit
    FROM prepared s
    LEFT JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at
    FROM combined_details s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
source_stats AS (
    -- Avg ms_played_bucket per source
    SELECT source, AVG(ms_played_bucket) AS avg_ms_played_bucket
    FROM with_stats
    GROUP BY source
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played_bucket,
        ROUND(s.ms_played_bucket - g.avg_ms_played_bucket, 2) AS ms_played_bucket_minus_avg
    FROM with_stats s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played_bucket > g.avg_ms_played_bucket
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        track_id,
        device,
        source,
        ms_played_bucket,
        is_skipped_flag,
        ms_played_tier,
        listener_plan,
        listener_interests,
        track_is_explicit,
        last_played_at,
        avg_ms_played_bucket,
        ms_played_bucket_minus_avg,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY ms_played_tier ORDER BY last_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        MAX(avg_ms_played_bucket) OVER (PARTITION BY track_is_explicit, device ORDER BY ms_played_bucket DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS avg_ms_played_bucket_max_2_prior,
        MIN(ms_played_bucket_minus_avg) OVER (PARTITION BY ms_played_tier) AS ms_played_tier_min_ms_played_bucket_minus_avg,
        COUNT(*) OVER (PARTITION BY source, device ORDER BY last_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(avg_ms_played_bucket) OVER (PARTITION BY source, device ORDER BY last_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_avg_ms_played_bucket
    FROM compared
),
stats AS (
    -- Summarise per group
    SELECT
        is_skipped_flag,
        track_id,
        ms_played_tier,
        COUNT(*) FILTER (WHERE track_is_explicit = TRUE) AS n_track_is_explicit_true,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        ROUND(100 * COUNT(CASE WHEN listener_plan = 'free' THEN 1 END) / COUNT(*), 1) AS pct_listener_plan_free,
        COUNT(*) AS n_rows
    FROM sequenced
    GROUP BY 1, 2, 3
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY ms_played_tier) AS rows_in_ms_played_tier,
        FIRST_VALUE(is_skipped_flag) IGNORE NULLS OVER (PARTITION BY n_rows, track_id ORDER BY n_rows ASC, is_skipped_flag, track_id, ms_played_tier ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_skipped_flag,
        LAG(pct_listener_plan_free) OVER w_n_rows_track_id_by_n_rows AS prev_pct_listener_plan_free,
        MAX(pct_listener_plan_free) OVER w_ms_played_tier AS ms_played_tier_max_pct_listener_plan_free
    FROM stats
    WINDOW w_n_rows_track_id_by_n_rows AS (PARTITION BY n_rows, track_id ORDER BY n_rows ASC, is_skipped_flag, track_id, ms_played_tier), w_ms_played_tier AS (PARTITION BY ms_played_tier)
)
SELECT
    is_skipped_flag,
    track_id,
    n_track_is_explicit_true,
    source_list,
    pct_listener_plan_free
FROM with_windows
ORDER BY n_track_is_explicit_true
