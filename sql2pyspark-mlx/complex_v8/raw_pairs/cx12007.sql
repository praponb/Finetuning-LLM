WITH recent AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE is_skipped = FALSE
      AND device = 'speaker'
),
with_parent_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        t.title AS track_title,
        t.moods AS track_moods,
        t.is_explicit AS track_is_explicit
    FROM recent s
    JOIN tracks t ON t.id = s.track_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        ms_played,
        device,
        is_skipped,
        track_title,
        track_moods,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY listener_id, is_skipped ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        LAST_VALUE(played_at) OVER (PARTITION BY device ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY device), 2) AS ms_played_vs_device_avg,
        MIN(ms_played) OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        FIRST_VALUE(ms_played) OVER (PARTITION BY listener_id, is_skipped ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        MIN(ms_played) OVER w_listener_id AS listener_id_min_ms_played,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id ORDER BY ms_played), 3) AS ms_played_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY device ORDER BY ms_played DESC NULLS FIRST), 3) AS ms_played_percent_rank,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY ms_played) AS ms_played_dense_rank,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id
    FROM with_parent_track
    WINDOW w_listener_id AS (PARTITION BY listener_id)
)
SELECT
    id,
    ms_played,
    device,
    is_skipped,
    track_title,
    track_moods,
    last_played_at,
    ms_played_vs_device_avg,
    min_ms_played_so_far,
    listener_id_min_ms_played,
    ms_played_cume_dist,
    ms_played_percent_rank,
    ms_played_dense_rank,
    rows_in_listener_id,
    ABS(rows_in_listener_id - 12) AS rows_in_listener_id_distance,
    FLOOR(min_ms_played_so_far / 1000000) * 1000000 AS min_ms_played_so_far_bucket
FROM with_windows
