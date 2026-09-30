WITH prepared_plays AS (
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
        CASE WHEN ms_played >= 225733 THEN 'large' WHEN ms_played >= 129336 THEN 'medium' ELSE 'small' END AS ms_played_tier,
        CONCAT(device, '#', CAST(listener_id AS STRING)) AS device_ref
    FROM plays
    WHERE MONTH(played_at) IN (1, 2)
      AND track_id >= 8
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        ROUND(PERCENT_RANK() OVER (ORDER BY ms_played DESC), 3) AS ms_played_percent_rank,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY device, ms_played_tier)) AS distinct_source_in_device_ms_played_tier,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY device, ms_played_tier), 0), 4) AS ms_played_share_of_device_ms_played_tier,
        MAX(ms_played) OVER w_device_ms_played_tier AS device_ms_played_tier_max_ms_played,
        ROW_NUMBER() OVER (PARTITION BY source, track_id ORDER BY played_at, id) AS played_at_seq,
        LEAD(played_at, 2) OVER (ORDER BY ms_played DESC, id) AS next2_played_at,
        ROUND(AVG(ms_played) OVER (PARTITION BY source, track_id ORDER BY played_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_3,
        COUNT(*) OVER (PARTITION BY device, ms_played_tier) AS rows_in_device_ms_played_tier,
        RANK() OVER (ORDER BY ms_played DESC, id) AS ms_played_rank,
        FIRST_VALUE(played_at) OVER (ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at
    FROM prepared_plays
    WINDOW w_device_ms_played_tier AS (PARTITION BY device, ms_played_tier)
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    device,
    is_skipped,
    ms_played_percent_rank,
    ms_played_share_of_device_ms_played_tier,
    device_ms_played_tier_max_ms_played,
    next2_played_at,
    ms_played_avg_3,
    rows_in_device_ms_played_tier,
    ms_played_rank,
    TO_DATE(first_played_at) AS first_played_at_date
FROM with_windows
ORDER BY ms_played_rank
