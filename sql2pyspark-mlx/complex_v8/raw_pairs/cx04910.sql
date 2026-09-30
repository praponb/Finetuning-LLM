WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
    WHERE is_skipped = FALSE
      AND ms_played <= 239955
),
combined_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        t.bpm AS track_bpm
    FROM base s
    JOIN tracks t ON t.id = s.track_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        track_bpm,
        ROUND(AVG(track_bpm) OVER (ORDER BY track_bpm DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS track_bpm_avg_7,
        ROUND(AVG(track_bpm) OVER (ORDER BY track_bpm, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS track_bpm_avg_3_prior,
        SIZE(COLLECT_SET(listener_id) OVER w_source) AS distinct_listener_id_in_source,
        COUNT(*) OVER (PARTITION BY source) AS rows_in_source,
        ROUND(AVG(track_bpm) OVER (ORDER BY track_bpm, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS track_bpm_avg_3,
        MAX(track_bpm) OVER (ORDER BY track_bpm DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS track_bpm_max_6_prior,
        MIN(track_bpm) OVER w_source AS source_min_track_bpm,
        ROW_NUMBER() OVER (ORDER BY track_bpm DESC NULLS FIRST, id) AS track_bpm_seq,
        ROW_NUMBER() OVER w_all_by_track_bpm_2 AS seq_no,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER w_source, 0), 4) AS track_bpm_share_of_source,
        FIRST_VALUE(track_bpm) OVER (ORDER BY track_bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_track_bpm,
        LAG(track_bpm) OVER (ORDER BY track_bpm DESC NULLS FIRST, id) AS prev_track_bpm,
        MIN(track_bpm) OVER (ORDER BY track_bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_track_bpm_so_far,
        FIRST_VALUE(track_bpm) IGNORE NULLS OVER (ORDER BY track_bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_track_bpm
    FROM combined_details_track
    WINDOW w_source AS (PARTITION BY source), w_all_by_track_bpm_2 AS (ORDER BY track_bpm, id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        is_skipped,
        track_bpm,
        track_bpm_avg_7,
        track_bpm_avg_3_prior,
        distinct_listener_id_in_source,
        rows_in_source,
        track_bpm_avg_3,
        track_bpm_max_6_prior,
        source_min_track_bpm,
        track_bpm_seq,
        first_track_bpm,
        prev_track_bpm,
        min_track_bpm_so_far,
        first_known_track_bpm,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY track_bpm_seq, is_skipped)) AS distinct_track_id_in_track_bpm_seq_is_skipped,
        SUM(track_bpm_avg_7) OVER w_track_bpm_seq_is_skipped AS track_bpm_seq_is_skipped_sum_track_bpm_avg_7,
        NTILE(10) OVER (ORDER BY track_bpm_share_of_source DESC, id) AS track_bpm_share_of_source_decile
    FROM with_trends
    WINDOW w_track_bpm_seq_is_skipped AS (PARTITION BY track_bpm_seq, is_skipped)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_listener_id_in_source,
        COUNT(*) AS n_rows,
        SUM(CASE WHEN NOT is_skipped THEN min_track_bpm_so_far ELSE 0 END) AS min_track_bpm_so_far_not_is_skipped
    FROM ranked
    GROUP BY distinct_listener_id_in_source
    HAVING SUM(track_bpm_seq_is_skipped_sum_track_bpm_avg_7) > 495.0
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(n_rows, 2) OVER (PARTITION BY distinct_listener_id_in_source ORDER BY min_track_bpm_so_far_not_is_skipped DESC NULLS FIRST, distinct_listener_id_in_source ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_rows,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_listener_id_in_source ORDER BY min_track_bpm_so_far_not_is_skipped DESC NULLS FIRST), 3) AS min_track_bpm_so_far_not_is_skipped_percent_rank
    FROM summary
)
SELECT
    distinct_listener_id_in_source,
    n_rows,
    min_track_bpm_so_far_not_is_skipped,
    min_track_bpm_so_far_not_is_skipped_percent_rank,
    FLOOR(n_rows / 2) * 2 AS n_rows_bucket,
    COALESCE(second_n_rows, 0) AS second_n_rows_filled
FROM with_windows
WHERE n_rows >= 3
