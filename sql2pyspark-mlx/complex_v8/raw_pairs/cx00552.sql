WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        CASE WHEN ms_played >= 308104 THEN 'A' WHEN ms_played >= 168782 THEN 'B' ELSE 'C' END AS ms_played_tier,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at
    FROM plays
    WHERE (ms_played >= 71476 OR listener_id IS NOT NULL)
),
with_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.ms_played_tier,
        s.days_since_played_at,
        l.username AS listener_username
    FROM prepared_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id AND l.country = 'TH'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(days_since_played_at) OVER (PARTITION BY is_skipped ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_days_since_played_at_so_far,
        MAX(ms_played) OVER w_track_id AS track_id_max_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY source, listener_id ORDER BY played_at ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_3_prior,
        COUNT(*) OVER (PARTITION BY is_skipped ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY track_id), 0), 4) AS ms_played_share_of_track_id,
        SUM(days_since_played_at) OVER (PARTITION BY is_skipped, track_id ORDER BY days_since_played_at ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS days_since_played_at_sum_4,
        NTILE(5) OVER (PARTITION BY is_skipped ORDER BY ms_played DESC, id) AS ms_played_quintile
    FROM with_details
    WINDOW w_track_id AS (PARTITION BY track_id)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        ms_played_tier,
        listener_username,
        min_days_since_played_at_so_far,
        track_id_max_ms_played,
        ms_played_avg_3_prior,
        running_count,
        ms_played_share_of_track_id,
        days_since_played_at_sum_4,
        ms_played_quintile,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref
    FROM metrics
    WHERE days_since_played_at <= 420
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        listener_username,
        min_days_since_played_at_so_far,
        running_count,
        ms_played_share_of_track_id,
        days_since_played_at_sum_4,
        ms_played_quintile,
        source_ref,
        SUM(ms_played_avg_3_prior) OVER (PARTITION BY track_id, ms_played_tier) AS track_id_ms_played_tier_sum_ms_played_avg_3_prio,
        MAX(days_since_played_at_sum_4) OVER (PARTITION BY listener_id, ms_played_tier ORDER BY ms_played_avg_3_prior DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_played_at_sum_4_max_6_prior,
        LAST_VALUE(days_since_played_at_sum_4) OVER (PARTITION BY ms_played_quintile ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_played_at_sum_4
    FROM scored
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    source,
    listener_username,
    min_days_since_played_at_so_far,
    running_count,
    ms_played_share_of_track_id,
    days_since_played_at_sum_4,
    source_ref,
    track_id_ms_played_tier_sum_ms_played_avg_3_prio,
    days_since_played_at_sum_4_max_6_prior,
    last_days_since_played_at_sum_4
FROM ranked
ORDER BY running_count NULLS LAST, id
LIMIT 5
