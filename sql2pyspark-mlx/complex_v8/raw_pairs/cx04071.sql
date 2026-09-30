WITH filtered_plays AS (
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
        COALESCE(ms_played, 0) AS ms_played_filled,
        CASE WHEN device IN ('tv', 'desktop') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
    WHERE ms_played <= 225733
      AND source IN ('radio', 'playlist', 'album')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id,
        MAX(ms_played_filled) OVER (PARTITION BY is_skipped ORDER BY ms_played, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ms_played_filled_max_2_prior,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY is_skipped_label ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        NTILE(4) OVER (PARTITION BY is_skipped ORDER BY ms_played, id) AS ms_played_quartile,
        ROUND(CUME_DIST() OVER (ORDER BY ms_played_filled), 3) AS ms_played_filled_cume_dist
    FROM filtered_plays
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        ms_played_filled,
        device_group,
        is_skipped_label,
        rows_in_listener_id,
        ms_played_filled_max_2_prior,
        second_played_at,
        MAX(rows_in_listener_id) OVER (PARTITION BY track_id, device_group) AS track_id_device_group_max_rows_in_listener_id,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY track_id, device_group)) AS distinct_source_in_track_id_device_group
    FROM with_trends
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(ms_played_filled, 2) OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_filled,
        NTILE(5) OVER w_all_by_played_at AS played_at_quintile,
        COUNT(*) OVER (PARTITION BY track_id, is_skipped_label) AS rows_in_track_id_is_skipped_label
    FROM metrics
    WINDOW w_all_by_played_at AS (ORDER BY played_at DESC, id)
)
SELECT *
FROM (
    SELECT played_at, distinct_source_in_track_id_device_group, device, ms_played_filled_max_2_prior
    FROM ranked
) src
PIVOT (
    SUM(ms_played_filled_max_2_prior)
    FOR device IN ('speaker', 'phone', 'tv', 'desktop')
)
ORDER BY played_at, distinct_source_in_track_id_device_group
