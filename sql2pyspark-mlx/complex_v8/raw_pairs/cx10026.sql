WITH filtered AS (
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
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        s.ms_played_filled,
        t.bpm AS track_bpm
    FROM filtered s
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(track_bpm, 2) OVER (PARTITION BY track_id ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_bpm,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY ms_played_filled NULLS LAST, id) AS rn,
        COUNT(*) OVER (PARTITION BY device ORDER BY ms_played_filled NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(track_bpm - AVG(track_bpm) OVER (PARTITION BY listener_id), 2) AS track_bpm_vs_listener_id_avg,
        MAX(ms_played_filled) OVER w_listener_id AS listener_id_max_ms_played_filled,
        ROUND(PERCENT_RANK() OVER w_device_by_ms_played_filled_t, 3) AS ms_played_filled_percent_rank,
        MAX(track_bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS track_bpm_max_3,
        ms_played - LAG(ms_played) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY ms_played DESC NULLS FIRST, id) AS ms_played_change,
        ROUND(ms_played_filled / NULLIF(SUM(ms_played_filled) OVER (PARTITION BY listener_id), 0), 4) AS ms_played_filled_share_of_listener_id,
        LAG(ms_played, 1, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY ms_played DESC NULLS FIRST, id) AS prev_ms_played,
        RANK() OVER w_played_at_month_by_ms_played_3 AS ms_played_rank,
        LAG(ms_played, 2) OVER (PARTITION BY device ORDER BY ms_played_filled NULLS LAST, id) AS prev2_ms_played,
        MIN(ms_played) OVER w_listener_id AS listener_id_min_ms_played,
        LEAD(ms_played) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY ms_played DESC NULLS FIRST, id) AS next_ms_played
    FROM combined_details
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_device_by_ms_played_filled_t AS (PARTITION BY device ORDER BY ms_played_filled NULLS LAST), w_played_at_month_by_ms_played_3 AS (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY ms_played DESC NULLS FIRST)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(prev2_ms_played / NULLIF(SUM(prev2_ms_played) OVER (PARTITION BY track_id, rn), 0), 4) AS prev2_ms_played_share_of_track_id_rn,
        MAX(ms_played) OVER (PARTITION BY source ORDER BY track_bpm_vs_listener_id_avg NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_max_7,
        MIN(ms_played) OVER (ORDER BY track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        DENSE_RANK() OVER (ORDER BY track_bpm, id) AS track_bpm_dense_rank,
        FIRST_VALUE(track_bpm_vs_listener_id_avg) IGNORE NULLS OVER (PARTITION BY source ORDER BY track_bpm_vs_listener_id_avg NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_track_bpm_vs_listener_id_avg,
        LAST_VALUE(prev_ms_played) OVER (ORDER BY track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prev_ms_played
    FROM with_windows
)
-- Final projection
SELECT
    id,
    track_id,
    played_at,
    source,
    second_track_bpm,
    rn,
    track_bpm_vs_listener_id_avg,
    listener_id_max_ms_played_filled,
    ms_played_filled_percent_rank,
    track_bpm_max_3,
    ms_played_change,
    ms_played_filled_share_of_listener_id,
    prev_ms_played,
    prev2_ms_played,
    listener_id_min_ms_played,
    next_ms_played,
    prev2_ms_played_share_of_track_id_rn,
    min_ms_played_so_far,
    track_bpm_dense_rank,
    last_prev_ms_played,
    LENGTH(TRIM(source)) AS source_len
FROM windowed
ORDER BY rn DESC, id
LIMIT 10
