WITH scoped AS (
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
        DATE_TRUNC('DAY', played_at) AS played_at_day
    FROM plays
    WHERE track_id <= 7
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_listener_id_by_ms_played_tie AS ms_played_dense_rank,
        DENSE_RANK() OVER w_device_listener_id_by_ms_pla AS ms_played_dense_rank_2,
        ROUND(ms_played - AVG(ms_played) OVER w_device, 2) AS ms_played_vs_device_avg,
        LAST_VALUE(played_at) OVER (PARTITION BY listener_id ORDER BY played_at_day ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        LAST_VALUE(played_at) OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at_2,
        RANK() OVER w_all_by_played_at_ties AS played_at_rank,
        MIN(ms_played) OVER (PARTITION BY device) AS device_min_ms_played,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        MAX(ms_played) OVER (PARTITION BY is_skipped ORDER BY ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_max_7,
        SUM(ms_played) OVER (PARTITION BY device, listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at_day ASC, id) AS prev_played_at,
        LAST_VALUE(played_at_day) IGNORE NULLS OVER (PARTITION BY listener_id ORDER BY played_at_day ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_day
    FROM scoped
    WINDOW w_listener_id_by_ms_played_tie AS (PARTITION BY listener_id ORDER BY ms_played DESC), w_device_listener_id_by_ms_pla AS (PARTITION BY device, listener_id ORDER BY ms_played, id), w_device AS (PARTITION BY device), w_all_by_played_at_ties AS (ORDER BY played_at ASC)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE ms_played_dense_rank = 1
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        played_at_day,
        ms_played_dense_rank_2,
        ms_played_vs_device_avg,
        last_played_at,
        last_played_at_2,
        rows_in_track_id,
        ms_played_max_7,
        running_ms_played,
        last_known_played_at_day,
        ROUND(ms_played_dense_rank - AVG(ms_played_dense_rank) OVER (PARTITION BY is_skipped, listener_id), 2) AS ms_played_dense_rank_vs_is_skipped_listener_id_a,
        DENSE_RANK() OVER w_source_is_skipped_by_ms_play AS ms_played_dense_rank_dense_rank,
        ROUND(AVG(device_min_ms_played) OVER (ORDER BY ms_played_max_7 DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS device_min_ms_played_avg_2,
        SUM(ms_played_dense_rank) OVER (ORDER BY device_min_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played_dense_rank
    FROM leaders
    WINDOW w_source_is_skipped_by_ms_play AS (PARTITION BY source, is_skipped ORDER BY ms_played_dense_rank DESC)
)
SELECT
    id,
    listener_id,
    played_at,
    source,
    played_at_day,
    ms_played_dense_rank_2,
    ms_played_vs_device_avg,
    last_played_at,
    last_played_at_2,
    rows_in_track_id,
    running_ms_played,
    last_known_played_at_day,
    ms_played_dense_rank_vs_is_skipped_listener_id_a,
    ms_played_dense_rank_dense_rank,
    device_min_ms_played_avg_2,
    running_ms_played_dense_rank
FROM sequenced
ORDER BY ms_played_dense_rank_vs_is_skipped_listener_id_a NULLS LAST, id
LIMIT 5
