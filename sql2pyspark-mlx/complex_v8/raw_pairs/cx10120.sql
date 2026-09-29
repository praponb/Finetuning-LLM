WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        QUARTER(played_at) AS played_at_quarter,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix
    FROM plays
    WHERE ms_played BETWEEN 30020 AND 359437
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.device_or_unknown,
        s.source_prefix,
        p.id AS play_id,
        p.source AS play_source,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played,
        p.device AS play_device
    FROM filtered_plays s
    JOIN plays p ON p.track_id = s.track_id AND p.source IS NOT NULL
),
device_or_unknown_stats AS (
    -- Max play_ms_played per device_or_unknown
    SELECT device_or_unknown, MAX(play_ms_played) AS max_play_ms_played
    FROM with_children_plays
    GROUP BY device_or_unknown
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_play_ms_played,
        ROUND(s.play_ms_played - g.max_play_ms_played, 2) AS play_ms_played_minus_max
    FROM with_children_plays s
    JOIN device_or_unknown_stats g ON g.device_or_unknown = s.device_or_unknown
    WHERE s.play_ms_played = g.max_play_ms_played
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source_prefix,
        s.play_id,
        s.play_listener_id,
        s.play_device,
        s.max_play_ms_played,
        s.play_ms_played_minus_max,
        p.id AS play_id_2,
        p.source AS play_source_2,
        p.listener_id AS play_listener_id_2,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device_2
    FROM vs_group s
    JOIN plays p ON p.track_id = s.track_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY play_id, listener_id ORDER BY played_at) AS played_at_rank,
        ROUND(play_ms_played_minus_max / NULLIF(SUM(play_ms_played_minus_max) OVER (PARTITION BY play_listener_id_2, play_is_skipped), 0), 4) AS play_ms_played_minus_max_share_of_play_listener_,
        DENSE_RANK() OVER (ORDER BY played_at DESC) AS played_at_dense_rank,
        SIZE(COLLECT_SET(track_id) OVER w_play_listener_id_2_play_is_s) AS distinct_track_id_in_play_listener_id_2_play_is_,
        COUNT(*) OVER (PARTITION BY play_listener_id_2, play_is_skipped) AS rows_in_play_listener_id_2_play_is_skipped
    FROM detail_rows_plays
    WINDOW w_play_listener_id_2_play_is_s AS (PARTITION BY play_listener_id_2, play_is_skipped)
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.play_listener_id_2
              AND p.device <> 'tv'
        )
      AND s.play_source_2 IS NOT NULL
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source_prefix,
        s.play_id,
        s.play_listener_id,
        s.play_ms_played_minus_max,
        s.play_id_2,
        s.play_listener_id_2,
        s.play_is_skipped,
        s.play_ms_played_minus_max_share_of_play_listener_,
        s.rows_in_play_listener_id_2_play_is_skipped,
        p.id AS play_id_3,
        p.is_skipped AS play_is_skipped_2,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.ms_played AS play_ms_played
    FROM screened s
    JOIN plays p ON p.listener_id = s.listener_id
)
SELECT *
FROM (
    SELECT play_listener_id_2, track_id, device, rows_in_play_listener_id_2_play_is_skipped
    FROM exploded_detail
) src
PIVOT (
    SUM(rows_in_play_listener_id_2_play_is_skipped)
    FOR device IN ('car', 'tv')
)
ORDER BY play_listener_id_2, track_id
