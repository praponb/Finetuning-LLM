WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm
    FROM tracks
    WHERE duration_sec < 409
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        MAX(bpm) OVER (ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS bpm_max_3_prior,
        ROUND(AVG(duration_sec) OVER (ORDER BY bpm DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_6_prior,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        DENSE_RANK() OVER (ORDER BY duration_sec) AS duration_sec_dense_rank,
        SUM(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_sum_duration_sec,
        NTH_VALUE(bpm, 2) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        DENSE_RANK() OVER w_all_by_bpm_ties AS bpm_dense_rank
    FROM base
    WINDOW w_all_by_bpm_ties AS (ORDER BY bpm DESC)
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(second_duration_sec) OVER (PARTITION BY bpm_dense_rank, is_explicit ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_second_duration_sec_so_far,
        ROUND(last_known_duration_sec / NULLIF(SUM(last_known_duration_sec) OVER (PARTITION BY bpm_dense_rank), 0), 4) AS last_known_duration_sec_share_of_bpm_dense_rank,
        ROUND(artist_id_sum_duration_sec - AVG(artist_id_sum_duration_sec) OVER (PARTITION BY artist_id), 2) AS artist_id_sum_duration_sec_vs_artist_id_avg,
        SUM(rows_in_is_explicit) OVER (PARTITION BY duration_sec_dense_rank ORDER BY last_known_duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rows_in_is_explicit_sum_2_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit, duration_sec_dense_rank ORDER BY artist_id_sum_duration_sec DESC), 3) AS artist_id_sum_duration_sec_percent_rank,
        MIN(last_known_duration_sec) OVER w_bpm_dense_rank AS bpm_dense_rank_min_last_known_duration_sec,
        second_duration_sec - LAG(second_duration_sec) OVER (PARTITION BY is_explicit, duration_sec_dense_rank ORDER BY artist_id_sum_duration_sec DESC, id) AS second_duration_sec_change
    FROM with_windows
    WINDOW w_bpm_dense_rank AS (PARTITION BY bpm_dense_rank)
)
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    bpm_max_3_prior,
    duration_sec_avg_6_prior,
    second_duration_sec,
    duration_sec_dense_rank,
    artist_id_sum_duration_sec,
    max_second_duration_sec_so_far,
    last_known_duration_sec_share_of_bpm_dense_rank,
    artist_id_sum_duration_sec_vs_artist_id_avg,
    rows_in_is_explicit_sum_2_prior,
    second_duration_sec_change,
    GREATEST(duration_sec_avg_6_prior, bpm) AS greatest_duration_sec_avg_6_prior_bpm
FROM with_windows_2
