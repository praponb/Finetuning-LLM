WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE listener_id >= 14
      AND device <> 'desktop'
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device_slug,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at,
        p.device AS play_device,
        p.source AS play_source
    FROM valid s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
),
per_group AS (
    -- Group-level totals
    SELECT
        YEAR(play_played_at) AS play_played_at_year,
        MAX(play_ms_played) AS max_play_ms_played,
        COUNT(*) AS cnt,
        ROUND(AVG(play_ms_played), 2) AS avg_play_ms_played,
        COUNT(DISTINCT track_id) AS distinct_track_ids,
        COUNT(DISTINCT play_device) AS distinct_play_devices
    FROM with_children_plays
    GROUP BY YEAR(play_played_at)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(distinct_track_ids) OVER (PARTITION BY distinct_play_devices) AS distinct_play_devices_min_distinct_track_ids,
        NTH_VALUE(cnt, 2) OVER (ORDER BY max_play_ms_played ASC, play_played_at_year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_cnt,
        NTILE(10) OVER (ORDER BY max_play_ms_played ASC, play_played_at_year) AS max_play_ms_played_decile,
        SUM(play_played_at_year) OVER (PARTITION BY distinct_play_devices) AS distinct_play_devices_sum_play_played_at_year,
        ROUND(AVG(max_play_ms_played) OVER (PARTITION BY distinct_play_devices), 2) AS distinct_play_devices_avg_max_play_ms_played,
        ROUND(cnt - AVG(cnt) OVER (PARTITION BY distinct_play_devices), 2) AS cnt_vs_distinct_play_devices_avg
    FROM per_group
)
SELECT play_played_at_year, field, metric_value
FROM (
    SELECT play_played_at_year, second_cnt, distinct_play_devices, distinct_play_devices_sum_play_played_at_year, max_play_ms_played_decile
    FROM windowed
) src
UNPIVOT (
    metric_value FOR field IN (second_cnt, distinct_play_devices, distinct_play_devices_sum_play_played_at_year, max_play_ms_played_decile)
) unp
ORDER BY play_played_at_year, field
