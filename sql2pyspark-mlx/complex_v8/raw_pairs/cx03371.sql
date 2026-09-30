WITH base_plays AS (
    -- Filter plays
    SELECT *
    FROM plays
    WHERE source NOT IN ('radio', 'playlist')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ms_played - LAG(ms_played) OVER w_all_by_ms_played AS ms_played_change,
        ROUND(ms_played - AVG(ms_played) OVER w_track_id_listener_id, 2) AS ms_played_vs_track_id_listener_id_avg,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY is_skipped, source ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        COUNT(*) OVER w_track_id_listener_id AS rows_in_track_id_listener_id,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (ORDER BY ms_played NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        MAX(ms_played) OVER (PARTITION BY track_id) AS track_id_max_ms_played,
        SUM(ms_played) OVER w_track_id_listener_id AS track_id_listener_id_sum_ms_played,
        MIN(ms_played) OVER (ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        LAST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id), 2) AS track_id_avg_ms_played,
        NTH_VALUE(played_at, 2) OVER (ORDER BY ms_played NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        LAG(played_at, 3) OVER (ORDER BY ms_played ASC, id) AS prev3_played_at
    FROM base_plays
    WINDOW w_all_by_ms_played AS (ORDER BY ms_played NULLS LAST, id), w_track_id_listener_id AS (PARTITION BY track_id, listener_id)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        ms_played_vs_track_id_listener_id_avg,
        second_ms_played,
        rows_in_track_id_listener_id,
        track_id_max_ms_played,
        min_ms_played_so_far,
        last_known_played_at,
        track_id_avg_ms_played,
        prev3_played_at,
        ROUND(CUME_DIST() OVER w_is_skipped_by_prev3_played_a, 3) AS prev3_played_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(second_played_at) - UNIX_TIMESTAMP(LAG(second_played_at) OVER (PARTITION BY device ORDER BY track_id_listener_id_sum_ms_played, id))) / 60, 1) AS minutes_since_prev_second_played_at,
        SUM(rows_in_track_id_listener_id) OVER (PARTITION BY device ORDER BY track_id_listener_id_sum_ms_played, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rows_in_track_id_listener_id_sum_2_prior
    FROM sequenced
    WINDOW w_is_skipped_by_prev3_played_a AS (PARTITION BY is_skipped ORDER BY prev3_played_at, id)
)
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    device,
    is_skipped,
    source,
    ms_played_vs_track_id_listener_id_avg,
    second_ms_played,
    rows_in_track_id_listener_id,
    track_id_max_ms_played,
    min_ms_played_so_far,
    last_known_played_at,
    track_id_avg_ms_played,
    prev3_played_at,
    prev3_played_at_cume_dist,
    minutes_since_prev_second_played_at,
    rows_in_track_id_listener_id_sum_2_prior
FROM sequenced_2
WHERE prev3_played_at >= TIMESTAMP '2025-01-31 10:00:00'
ORDER BY min_ms_played_so_far DESC
