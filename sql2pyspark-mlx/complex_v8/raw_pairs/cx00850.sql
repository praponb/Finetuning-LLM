WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE (device IS NOT NULL OR listener_id >= 2)
),
enriched_track AS (
    -- Enrich with dimension data
    SELECT s.*, t.duration_sec AS track_duration_sec
    FROM prepared s
    LEFT JOIN tracks t ON t.id = s.track_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(track_id) OVER w_listener_id) AS distinct_track_id_in_listener_id,
        LAG(track_duration_sec, 3) OVER w_is_skipped_by_track_duration AS prev3_track_duration_sec,
        RANK() OVER (PARTITION BY listener_id, is_skipped ORDER BY ms_played DESC, id) AS ms_played_rank,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY listener_id, is_skipped ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(ms_played - AVG(ms_played) OVER w_listener_id, 2) AS ms_played_vs_listener_id_avg,
        NTILE(3) OVER (PARTITION BY listener_id, is_skipped ORDER BY ms_played DESC, id) AS ms_played_tercile,
        MAX(track_duration_sec) OVER w_listener_id AS listener_id_max_track_duration_sec,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY listener_id), 0), 4) AS ms_played_share_of_listener_id,
        SUM(track_duration_sec) OVER w_listener_id AS listener_id_sum_track_duration_sec,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, is_skipped ORDER BY ms_played DESC), 3) AS ms_played_cume_dist,
        ROUND(track_duration_sec / NULLIF(SUM(track_duration_sec) OVER w_listener_id, 0), 4) AS track_duration_sec_share_of_listener_id
    FROM enriched_track
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_is_skipped_by_track_duration AS (PARTITION BY is_skipped ORDER BY track_duration_sec ASC, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(second_played_at) - UNIX_TIMESTAMP(LAG(second_played_at) OVER (PARTITION BY track_id, listener_id ORDER BY track_duration_sec_share_of_listener_id, id))) / 60, 1) AS minutes_since_prev_second_played_at,
        MIN(ms_played_rank) OVER (PARTITION BY ms_played_rank, is_skipped ORDER BY listener_id_max_track_duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_rank_so_far,
        LAST_VALUE(second_played_at) IGNORE NULLS OVER (PARTITION BY distinct_track_id_in_listener_id ORDER BY ms_played_tercile ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_second_played_at,
        ROUND(AVG(ms_played_tercile) OVER (PARTITION BY ms_played_tercile), 2) AS ms_played_tercile_avg_ms_played_tercile,
        DENSE_RANK() OVER (PARTITION BY distinct_track_id_in_listener_id ORDER BY ms_played_tercile ASC, id) AS ms_played_tercile_dense_rank,
        MIN(ms_played_vs_listener_id_avg) OVER (PARTITION BY ms_played_tercile) AS ms_played_tercile_min_ms_played_vs_listener_id_a
    FROM ranked
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    source,
    prev3_track_duration_sec,
    ms_played_rank,
    second_played_at,
    ms_played_vs_listener_id_avg,
    ms_played_tercile,
    ms_played_share_of_listener_id,
    listener_id_sum_track_duration_sec,
    ms_played_cume_dist,
    minutes_since_prev_second_played_at,
    min_ms_played_rank_so_far,
    ms_played_tercile_dense_rank,
    ms_played_tercile_min_ms_played_vs_listener_id_a,
    INITCAP(TRIM(source)) AS source_clean,
    ROUND((UNIX_TIMESTAMP(second_played_at) - UNIX_TIMESTAMP(played_at)) / 3600, 1) AS hours_played_at_to_second_played_at
FROM with_windows
ORDER BY source DESC, id
LIMIT 20
