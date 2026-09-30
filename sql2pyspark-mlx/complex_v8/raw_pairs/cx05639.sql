WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        CASE WHEN device IN ('speaker', 'car') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group,
        ABS(ms_played - 239955) AS ms_played_distance
    FROM plays
    WHERE track_id IS NOT NULL
      AND HOUR(played_at) BETWEEN 8 AND 11
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND HOUR(p.played_at) BETWEEN 8 AND 12
              AND p.device IS NOT NULL
        )
      AND s.device_group IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        played_at,
        device_group,
        ms_played_distance,
        ROUND(PERCENT_RANK() OVER w_track_id_listener_id_by_play, 3) AS played_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id, listener_id ORDER BY played_at), 3) AS played_at_cume_dist,
        COUNT(*) OVER w_listener_id AS rows_in_listener_id,
        ROUND(AVG(ms_played_distance) OVER w_listener_id, 2) AS listener_id_avg_ms_played_distance,
        LEAD(played_at) OVER (PARTITION BY track_id, listener_id ORDER BY played_at, id) AS next_played_at
    FROM screened
    WINDOW w_track_id_listener_id_by_play AS (PARTITION BY track_id, listener_id ORDER BY played_at), w_listener_id AS (PARTITION BY listener_id)
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.ms_played_distance,
        s.played_at_percent_rank,
        s.played_at_cume_dist,
        s.rows_in_listener_id,
        s.listener_id_avg_ms_played_distance,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped
    FROM ranked s
    JOIN plays p ON p.listener_id = s.listener_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        played_at_percent_rank,
        played_at_cume_dist,
        rows_in_listener_id,
        listener_id_avg_ms_played_distance,
        play_id,
        play_track_id,
        play_device,
        play_is_skipped,
        ROUND(CUME_DIST() OVER (ORDER BY played_at), 3) AS played_at_cume_dist_2,
        FIRST_VALUE(ms_played_distance) OVER (PARTITION BY listener_id ORDER BY ms_played_distance ASC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played_distance,
        SUM(played_at_percent_rank) OVER (PARTITION BY listener_id ORDER BY ms_played_distance ASC, id, play_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS played_at_percent_rank_sum_3
    FROM line_level
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.played_at_cume_dist,
        s.rows_in_listener_id,
        s.play_id,
        s.play_track_id,
        s.play_is_skipped,
        s.played_at_cume_dist_2,
        s.first_ms_played_distance,
        p.id AS play_id_2,
        p.played_at AS play_played_at,
        p.device AS play_device_2,
        p.track_id AS play_track_id_2,
        p.source AS play_source
    FROM windowed s
    LEFT JOIN plays p ON p.listener_id = s.listener_id AND p.source IN ('search', 'radio')
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count, COUNT(*) AS play_count_2
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_count_2, 0) AS play_count_2
    FROM detail_rows s
    LEFT JOIN play_stats p ON p.track_id = s.play_track_id_2
)
SELECT
    id,
    played_at_cume_dist,
    rows_in_listener_id,
    play_id,
    play_track_id,
    play_is_skipped,
    played_at_cume_dist_2,
    first_ms_played_distance,
    play_played_at,
    play_track_id_2,
    play_source,
    play_count,
    play_count_2
FROM with_counts
ORDER BY play_track_id_2 DESC NULLS FIRST
