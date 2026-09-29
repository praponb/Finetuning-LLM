WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source
    FROM plays
    WHERE ms_played > 52009
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        track_id,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        SUM(CASE WHEN source = 'playlist' THEN 1 ELSE 0 END) AS n_source_playlist,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        MAX(played_at) AS last_played_at,
        COUNT(*) FILTER (WHERE source IS NOT NULL) AS n_source_is_not_null,
        ROUND(AVG(ms_played), 2) AS avg_ms_played,
        SUM(CASE WHEN source = 'radio' THEN ms_played ELSE 0 END) AS ms_played_source_radio
    FROM base_plays
    WHERE track_id >= 3
    GROUP BY 1, 2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY ms_played_source_radio DESC, track_id, played_at_month), 3) AS ms_played_source_radio_percent_rank,
        RANK() OVER (PARTITION BY MONTH(played_at_month) ORDER BY last_played_at DESC) AS last_played_at_rank
    FROM per_group
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.track_id,
        s.played_at_month,
        s.source_list,
        s.last_played_at,
        s.avg_ms_played,
        s.ms_played_source_radio,
        s.last_played_at_rank,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.device AS play_device
    FROM ranked s
    LEFT JOIN plays p ON p.track_id = s.track_id
),
latest AS (
    -- Keep the earliest row per play_device, play_listener_id
    SELECT
        track_id,
        played_at_month,
        source_list,
        last_played_at,
        avg_ms_played,
        ms_played_source_radio,
        last_played_at_rank,
        play_id,
        play_listener_id,
        play_device
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY play_device, play_listener_id ORDER BY last_played_at, track_id, played_at_month, play_id) AS recency_rank
        FROM with_children_plays
    ) ranked
    WHERE recency_rank = 1
),
derived AS (
    -- Add calculated columns
    SELECT
        track_id,
        played_at_month,
        source_list,
        last_played_at,
        avg_ms_played,
        last_played_at_rank,
        play_id,
        play_device,
        UPPER(TRIM(play_device)) AS play_device_clean,
        HOUR(last_played_at) AS last_played_at_hour
    FROM latest
)
-- Final projection
SELECT
    track_id,
    source_list,
    last_played_at_rank,
    play_id,
    play_device,
    play_device_clean,
    last_played_at_rank + last_played_at_hour AS last_played_at_rank_plus_last_played_at_hour
FROM derived
ORDER BY play_device_clean DESC, play_device
