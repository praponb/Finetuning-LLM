WITH base_plays AS (
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
        INITCAP(TRIM(source)) AS source_clean
    FROM plays
    WHERE source IN ('album', 'playlist')
      AND TO_DATE(played_at) < DATE '2025-02-24'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        source_clean,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY device ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        LAST_VALUE(ms_played) OVER (PARTITION BY device, DATE_TRUNC('MONTH', played_at) ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        MAX(ms_played) OVER (PARTITION BY device ORDER BY ms_played ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ms_played_max_3_prior,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        ROW_NUMBER() OVER w_track_id_by_played_at AS played_at_seq,
        SUM(ms_played) OVER (PARTITION BY listener_id) AS listener_id_sum_ms_played,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id ORDER BY played_at DESC), 3) AS played_at_percent_rank
    FROM base_plays
    WINDOW w_track_id_by_played_at AS (PARTITION BY track_id ORDER BY played_at DESC, id)
),
annotated AS (
    -- Derive helper columns
    SELECT *, INITCAP(TRIM(device)) AS device_clean
    FROM with_windows
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        source_clean,
        second_played_at,
        last_ms_played,
        played_at_seq,
        played_at_percent_rank,
        LEAD(played_at_seq) OVER (PARTITION BY listener_id, device ORDER BY first_known_played_at DESC, id) AS next_played_at_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_played_at) ORDER BY second_played_at, id), 3) AS second_played_at_percent_rank,
        ROUND(AVG(played_at_seq) OVER (PARTITION BY played_at_seq, device), 2) AS played_at_seq_device_avg_played_at_seq,
        COUNT(*) OVER (PARTITION BY played_at_seq, device) AS rows_in_played_at_seq_device,
        NTILE(10) OVER w_listener_id_device_by_first AS first_known_played_at_decile
    FROM annotated
    WINDOW w_listener_id_device_by_first AS (PARTITION BY listener_id, device ORDER BY first_known_played_at DESC, id)
)
SELECT
    id,
    played_at,
    ms_played,
    device,
    is_skipped,
    source,
    second_played_at,
    played_at_seq,
    played_at_percent_rank,
    next_played_at_seq,
    played_at_seq_device_avg_played_at_seq,
    rows_in_played_at_seq_device,
    first_known_played_at_decile,
    ROUND(played_at_percent_rank * 0.9, 2) AS played_at_percent_rank_adj
FROM ranked
ORDER BY played_at_seq DESC
