WITH valid_plays AS (
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
    WHERE played_at < TIMESTAMP '2025-02-24 09:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        device,
        ROW_NUMBER() OVER w_all_by_ms_played AS ms_played_seq,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY is_skipped, MONTH(played_at)), 2) AS ms_played_vs_is_skipped_played_at_month_avg,
        SUM(ms_played) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played
    FROM valid_plays
    WINDOW w_all_by_ms_played AS (ORDER BY ms_played DESC, id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        played_at,
        device,
        ms_played_seq,
        ms_played_vs_is_skipped_played_at_month_avg,
        ROUND(PERCENT_RANK() OVER w_device_ms_played_seq_by_play, 3) AS played_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY device, ms_played_seq ORDER BY played_at NULLS LAST) AS played_at_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY device, ms_played_seq ORDER BY played_at NULLS LAST), 3) AS played_at_percent_rank_2
    FROM metrics
    WINDOW w_device_ms_played_seq_by_play AS (PARTITION BY device, ms_played_seq ORDER BY played_at NULLS LAST, id)
),
joined AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.device,
        s.ms_played_seq,
        s.ms_played_vs_is_skipped_played_at_month_avg,
        s.played_at_percent_rank,
        s.played_at_percent_rank_2,
        l.interests AS listener_interests,
        l.signup_date AS listener_signup_date
    FROM ranked s
    JOIN listeners l ON l.id = s.listener_id
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        MAX(ms_played) AS max_play_ms_played,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played
    FROM plays
    GROUP BY listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.max_play_ms_played,
        p.avg_play_ms_played
    FROM joined s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
play_stats_2 AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) AS play_count,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played_2,
        MIN(ms_played) AS min_play_ms_played
    FROM plays
    GROUP BY listener_id
),
profiled_2 AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        p.avg_play_ms_played_2,
        p.min_play_ms_played
    FROM profiled s
    LEFT JOIN play_stats_2 p ON p.listener_id = s.listener_id
)
SELECT
    id,
    listener_id,
    played_at,
    device,
    ms_played_seq,
    ms_played_vs_is_skipped_played_at_month_avg,
    played_at_percent_rank,
    played_at_percent_rank_2,
    listener_interests,
    listener_signup_date,
    max_play_ms_played,
    avg_play_ms_played,
    play_count,
    min_play_ms_played
FROM profiled_2
ORDER BY play_count
