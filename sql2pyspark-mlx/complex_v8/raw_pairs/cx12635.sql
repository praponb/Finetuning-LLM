WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        ADD_MONTHS(release_date, 3) AS release_date_plus_3m
    FROM tracks
    WHERE artist_id <= 5
      AND ARRAY_CONTAINS(moods, 'focus')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(duration_sec_per_bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_per_bpm_avg_2_prior,
        NTILE(5) OVER (ORDER BY bpm, id) AS bpm_quintile,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec ASC) AS duration_sec_dense_rank,
        MAX(duration_sec_per_bpm) OVER (ORDER BY bpm, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_per_bpm_max_3,
        ROW_NUMBER() OVER w_release_date_plus_3m_month_b AS duration_sec_per_bpm_seq,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id) AS rows_in_is_explicit_artist_id,
        NTILE(10) OVER (ORDER BY bpm, id) AS bpm_decile,
        ROW_NUMBER() OVER (ORDER BY bpm, id) AS rn,
        ROUND(CUME_DIST() OVER w_all_by_bpm, 3) AS bpm_cume_dist,
        LEAD(duration_sec_per_bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec ASC, id) AS next_duration_sec_per_bpm,
        RANK() OVER w_all_by_bpm_ties AS bpm_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec ASC), 3) AS duration_sec_percent_rank,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        ROUND(PERCENT_RANK() OVER (ORDER BY bpm, id), 3) AS bpm_percent_rank
    FROM recent
    WINDOW w_release_date_plus_3m_month_b AS (PARTITION BY MONTH(release_date_plus_3m) ORDER BY duration_sec_per_bpm ASC, id), w_all_by_bpm AS (ORDER BY bpm, id), w_all_by_bpm_ties AS (ORDER BY bpm)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        duration_sec_per_bpm_avg_2_prior,
        bpm_quintile,
        duration_sec_per_bpm_max_3,
        duration_sec_per_bpm_seq,
        rows_in_is_explicit_artist_id,
        bpm_decile,
        bpm_cume_dist,
        next_duration_sec_per_bpm,
        bpm_rank,
        duration_sec_percent_rank,
        first_known_bpm,
        bpm_percent_rank,
        LOWER(TRIM(title)) AS title_clean
    FROM sequenced
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(first_known_bpm) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), duration_sec_per_bpm_seq ORDER BY bpm_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_bpm,
        NTH_VALUE(duration_sec_percent_rank, 2) OVER (ORDER BY bpm_percent_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_percent_rank,
        ROUND(first_known_bpm / NULLIF(SUM(first_known_bpm) OVER (PARTITION BY bpm_rank, bpm_decile), 0), 4) AS first_known_bpm_share_of_bpm_rank_bpm_decile,
        SUM(bpm_quintile) OVER (PARTITION BY bpm_decile, artist_id ORDER BY first_known_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm_quintile,
        DENSE_RANK() OVER w_all_by_bpm_percent_rank_ties AS bpm_percent_rank_dense_rank
    FROM scored
    WINDOW w_all_by_bpm_percent_rank_ties AS (ORDER BY bpm_percent_rank ASC)
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    is_explicit,
    duration_sec_per_bpm_avg_2_prior,
    bpm_quintile,
    duration_sec_per_bpm_max_3,
    duration_sec_per_bpm_seq,
    rows_in_is_explicit_artist_id,
    bpm_cume_dist,
    next_duration_sec_per_bpm,
    bpm_rank,
    duration_sec_percent_rank,
    first_known_bpm,
    bpm_percent_rank,
    title_clean,
    second_duration_sec_percent_rank,
    running_bpm_quintile,
    bpm_percent_rank_dense_rank,
    CONCAT_WS('-', title_clean, title) AS title_clean_title_label,
    INITCAP(TRIM(title_clean)) AS title_clean_clean
FROM with_windows
ORDER BY id DESC NULLS FIRST, title_clean_title_label DESC
