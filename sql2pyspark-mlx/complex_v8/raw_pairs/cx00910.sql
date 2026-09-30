WITH cleaned AS (
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
        CONCAT_WS('-', source, device) AS source_device_label,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label
    FROM plays
    WHERE device = 'desktop'
      AND source = 'album'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        source,
        ROW_NUMBER() OVER w_all_by_played_at AS played_at_seq,
        SUM(ms_played) OVER w_source AS source_sum_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER w_source, 2) AS ms_played_vs_source_avg,
        NTILE(5) OVER w_all_by_ms_played AS ms_played_quintile,
        MAX(ms_played) OVER (PARTITION BY is_skipped) AS is_skipped_max_ms_played,
        RANK() OVER (ORDER BY played_at ASC) AS played_at_rank,
        SUM(ms_played) OVER (PARTITION BY is_skipped, track_id ORDER BY played_at ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ms_played_sum_6_prior,
        NTILE(10) OVER (PARTITION BY source, DATE_TRUNC('MONTH', played_at) ORDER BY played_at DESC, id) AS played_at_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY source, DATE_TRUNC('MONTH', played_at) ORDER BY played_at DESC), 3) AS played_at_percent_rank,
        COUNT(*) OVER (PARTITION BY source, DATE_TRUNC('MONTH', played_at) ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER (ORDER BY ms_played) AS ms_played_dense_rank,
        LAST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        ROW_NUMBER() OVER (ORDER BY ms_played DESC NULLS FIRST, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER w_all_by_played_at_ties, 3) AS played_at_percent_rank_2
    FROM cleaned
    WINDOW w_all_by_played_at AS (ORDER BY played_at ASC, id), w_source AS (PARTITION BY source), w_all_by_ms_played AS (ORDER BY ms_played DESC NULLS FIRST, id), w_all_by_played_at_ties AS (ORDER BY played_at ASC)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY seq_no, ms_played_dense_rank) AS rows_in_seq_no_ms_played_dense_rank,
        ROUND(ms_played_vs_source_avg - AVG(ms_played_vs_source_avg) OVER w_seq_no_ms_played_dense_rank, 2) AS ms_played_vs_source_avg_vs_seq_no_ms_played_dens,
        running_count - LAG(running_count) OVER (PARTITION BY listener_id, seq_no ORDER BY ms_played_vs_source_avg NULLS LAST, id) AS running_count_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY ms_played_quintile DESC NULLS FIRST, id), 3) AS ms_played_quintile_percent_rank,
        ms_played_dense_rank - LAG(ms_played_dense_rank) OVER (PARTITION BY listener_id, seq_no ORDER BY ms_played_vs_source_avg NULLS LAST, id) AS ms_played_dense_rank_change,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY seq_no, ms_played_dense_rank)) AS distinct_source_in_seq_no_ms_played_dense_rank
    FROM ranked
    WINDOW w_seq_no_ms_played_dense_rank AS (PARTITION BY seq_no, ms_played_dense_rank)
)
SELECT
    id,
    played_at,
    ms_played,
    played_at_seq,
    ms_played_vs_source_avg,
    is_skipped_max_ms_played,
    played_at_rank,
    played_at_decile,
    played_at_percent_rank,
    running_count,
    ms_played_dense_rank,
    last_known_played_at,
    played_at_percent_rank_2,
    rows_in_seq_no_ms_played_dense_rank,
    ms_played_vs_source_avg_vs_seq_no_ms_played_dens,
    ms_played_dense_rank_change,
    distinct_source_in_seq_no_ms_played_dense_rank,
    LENGTH(TRIM(source)) AS source_len
FROM with_windows
WHERE played_at_seq >= 3
ORDER BY id DESC, played_at
