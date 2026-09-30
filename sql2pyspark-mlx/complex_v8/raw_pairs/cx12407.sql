WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        played_at + INTERVAL 30 MINUTES AS played_at_plus_30m,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-13 14:00:00'
),
with_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.played_at_plus_30m,
        s.source_prefix,
        l.username AS listener_username
    FROM cleaned s
    JOIN listeners l ON l.id = s.listener_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY played_at_plus_30m) AS played_at_plus_30m_rank,
        LAST_VALUE(played_at_plus_30m) OVER (ORDER BY played_at_plus_30m, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at_plus_30m,
        FIRST_VALUE(played_at_plus_30m) IGNORE NULLS OVER (ORDER BY played_at_plus_30m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_plus_30m,
        LAG(played_at_plus_30m) OVER w_track_id_by_played_at_plus_3 AS prev_played_at_plus_30m,
        DENSE_RANK() OVER (PARTITION BY listener_id ORDER BY played_at_plus_30m DESC NULLS FIRST) AS played_at_plus_30m_dense_rank,
        FIRST_VALUE(played_at_plus_30m) IGNORE NULLS OVER (PARTITION BY listener_id ORDER BY played_at_plus_30m DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_plus_30m_2,
        LEAD(played_at_plus_30m, 2) OVER (ORDER BY played_at_plus_30m, id) AS next2_played_at_plus_30m,
        NTILE(4) OVER (PARTITION BY listener_id ORDER BY played_at_plus_30m DESC NULLS FIRST, id) AS played_at_plus_30m_quartile,
        NTILE(4) OVER w_track_id_by_played_at_plus_3_2 AS played_at_plus_30m_quartile_2,
        LAST_VALUE(played_at_plus_30m) IGNORE NULLS OVER (ORDER BY played_at_plus_30m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_plus_30m,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY played_at_plus_30m NULLS LAST) AS played_at_plus_30m_dense_rank_2,
        ROW_NUMBER() OVER w_listener_id_by_played_at_plu_4 AS played_at_plus_30m_seq,
        ROUND((UNIX_TIMESTAMP(played_at_plus_30m) - UNIX_TIMESTAMP(LAG(played_at_plus_30m) OVER w_listener_id_by_played_at_plu_3)) / 60, 1) AS minutes_since_prev_played_at_plus_30m,
        LAG(played_at_plus_30m) OVER (PARTITION BY listener_id ORDER BY played_at_plus_30m DESC, id) AS prev_played_at_plus_30m_2
    FROM with_details_listener
    WINDOW w_track_id_by_played_at_plus_3 AS (PARTITION BY track_id ORDER BY played_at_plus_30m DESC, id), w_track_id_by_played_at_plus_3_2 AS (PARTITION BY track_id ORDER BY played_at_plus_30m NULLS LAST, id), w_listener_id_by_played_at_plu_4 AS (PARTITION BY listener_id ORDER BY played_at_plus_30m DESC, id), w_listener_id_by_played_at_plu_3 AS (PARTITION BY listener_id ORDER BY played_at_plus_30m DESC NULLS FIRST, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        listener_id,
        track_id,
        device,
        played_at_plus_30m,
        source_prefix,
        listener_username,
        played_at_plus_30m_rank,
        last_played_at_plus_30m,
        first_known_played_at_plus_30m,
        prev_played_at_plus_30m,
        first_known_played_at_plus_30m_2,
        next2_played_at_plus_30m,
        played_at_plus_30m_quartile,
        played_at_plus_30m_quartile_2,
        last_known_played_at_plus_30m,
        played_at_plus_30m_dense_rank_2,
        played_at_plus_30m_seq,
        minutes_since_prev_played_at_plus_30m,
        prev_played_at_plus_30m_2
    FROM windowed
    WHERE played_at_plus_30m_dense_rank <= 5
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_played_at_plus_30m_seq_by_pl, 3) AS played_at_plus_30m_percent_rank,
        SIZE(COLLECT_SET(device) OVER w_prev_played_at_plus_30m_mont) AS distinct_device_in_prev_played_at_plus_30m_month,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_played_at_plus_30m)) AS rows_in_prev_played_at_plus_30m_month,
        MAX(played_at_plus_30m_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_played_at_plus_30m)) AS prev_played_at_plus_30m_month_max_played_at_plus
    FROM first_rows
    WINDOW w_played_at_plus_30m_seq_by_pl AS (PARTITION BY played_at_plus_30m_seq ORDER BY played_at_plus_30m DESC NULLS FIRST), w_prev_played_at_plus_30m_mont AS (PARTITION BY DATE_TRUNC('MONTH', prev_played_at_plus_30m))
)
SELECT
    id,
    listener_id,
    device,
    source_prefix,
    last_played_at_plus_30m,
    first_known_played_at_plus_30m,
    prev_played_at_plus_30m,
    first_known_played_at_plus_30m_2,
    next2_played_at_plus_30m,
    played_at_plus_30m_quartile_2,
    last_known_played_at_plus_30m,
    played_at_plus_30m_dense_rank_2,
    played_at_plus_30m_seq,
    minutes_since_prev_played_at_plus_30m,
    rows_in_prev_played_at_plus_30m_month,
    prev_played_at_plus_30m_month_max_played_at_plus
FROM with_trends
ORDER BY source_prefix, played_at_plus_30m_seq
