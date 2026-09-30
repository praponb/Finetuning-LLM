WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source
    FROM plays
    WHERE listener_id IS NOT NULL
      AND source <> 'radio'
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) FILTER (WHERE device NOT IN ('phone', 'car')) AS play_device_count,
        MAX(ms_played) AS max_play_ms_played,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_device_count, 0) AS play_device_count,
        p.max_play_ms_played,
        COALESCE(p.play_count, 0) AS play_count
    FROM cleaned s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(play_count, 0) AS play_count_filled,
        CONCAT_WS('|', source, device) AS source_device_label
    FROM summarized_rows
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ms_played,
        device,
        source,
        max_play_ms_played,
        play_count,
        play_count_filled,
        source_device_label,
        NTILE(5) OVER (ORDER BY play_device_count DESC, id) AS play_device_count_quintile,
        COUNT(*) OVER w_device_play_device_count AS rows_in_device_play_device_count,
        MIN(play_device_count) OVER (PARTITION BY device, play_device_count) AS device_play_device_count_min_play_device_count,
        MIN(ms_played) OVER (PARTITION BY device, play_device_count) AS device_play_device_count_min_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY device, track_id ORDER BY play_count_filled NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_4
    FROM tagged
    WINDOW w_device_play_device_count AS (PARTITION BY device, play_device_count)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(play_device_count_quintile) OVER (PARTITION BY play_count) AS play_count_max_play_device_count_quintile,
        RANK() OVER (PARTITION BY play_count ORDER BY play_device_count_quintile) AS play_device_count_quintile_rank,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY play_count)) AS distinct_device_in_play_count
    FROM ranked
),
tagged_2 AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN play_count_filled >= 7 THEN 'large' WHEN play_count_filled >= 2 THEN 'medium' ELSE 'small' END AS play_count_filled_tier
    FROM metrics
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(play_count, 2) OVER (ORDER BY device_play_device_count_min_play_device_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_play_count,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY play_device_count_quintile_rank, source)) AS distinct_device_in_play_device_count_quintile_ra,
        NTH_VALUE(max_play_ms_played, 2) OVER (PARTITION BY play_count_filled ORDER BY rows_in_device_play_device_count DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_play_ms_played,
        DENSE_RANK() OVER (ORDER BY device_play_device_count_min_play_device_count) AS device_play_device_count_min_play_device_count_d,
        NTH_VALUE(ms_played, 2) OVER (ORDER BY device_play_device_count_min_play_device_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        COUNT(*) OVER (PARTITION BY play_device_count_quintile_rank, source) AS rows_in_play_device_count_quintile_rank_source
    FROM tagged_2
)
SELECT
    id,
    ms_played,
    device,
    source,
    max_play_ms_played,
    play_count_filled,
    play_device_count_quintile,
    rows_in_device_play_device_count,
    device_play_device_count_min_play_device_count,
    ms_played_avg_4,
    play_count_max_play_device_count_quintile,
    play_device_count_quintile_rank,
    distinct_device_in_play_count,
    play_count_filled_tier,
    second_play_count,
    distinct_device_in_play_device_count_quintile_ra,
    device_play_device_count_min_play_device_count_d,
    second_ms_played,
    rows_in_play_device_count_quintile_rank_source,
    ROUND(ms_played_avg_4 * 0.9, 2) AS ms_played_avg_4_adj,
    CONCAT_WS('|', source_device_label, play_count_filled_tier) AS source_device_label_play_count_filled_tier_label
FROM with_windows
ORDER BY play_count_filled DESC, play_device_count_quintile_rank
