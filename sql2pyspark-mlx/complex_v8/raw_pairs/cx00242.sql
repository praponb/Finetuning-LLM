WITH base_tracks AS (
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
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE release_date < DATE '2025-08-07'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id) AS seq_no,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY YEAR(release_date) ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST), 3) AS duration_sec_percent_rank,
        LEAD(duration_sec, 1, 0) OVER (PARTITION BY YEAR(release_date) ORDER BY duration_sec NULLS LAST, id) AS next_duration_sec,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit ORDER BY release_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_3_prior,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit)) AS distinct_artist_id_in_is_explicit,
        ROUND(duration_sec - AVG(duration_sec) OVER w_is_explicit, 2) AS duration_sec_vs_is_explicit_avg,
        MAX(bpm) OVER (PARTITION BY YEAR(release_date) ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_so_far,
        ROUND(AVG(duration_sec) OVER w_is_explicit, 2) AS is_explicit_avg_duration_sec,
        NTILE(3) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id) AS duration_sec_tercile,
        MAX(duration_sec) OVER w_is_explicit AS is_explicit_max_duration_sec,
        NTILE(10) OVER (PARTITION BY YEAR(release_date) ORDER BY duration_sec NULLS LAST, id) AS duration_sec_decile,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY YEAR(release_date) ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        LEAD(release_date, 2) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id) AS next2_release_date,
        SUM(bpm) OVER (PARTITION BY is_explicit ORDER BY release_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS bpm_sum_7
    FROM base_tracks
    WINDOW w_is_explicit AS (PARTITION BY is_explicit)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        title_prefix,
        seq_no,
        second_bpm,
        duration_sec_percent_rank,
        next_duration_sec,
        bpm_avg_3_prior,
        distinct_artist_id_in_is_explicit,
        duration_sec_vs_is_explicit_avg,
        max_bpm_so_far,
        duration_sec_tercile,
        is_explicit_max_duration_sec,
        duration_sec_decile,
        first_duration_sec,
        next2_release_date,
        bpm_sum_7,
        RANK() OVER (PARTITION BY duration_sec_decile, seq_no ORDER BY next_duration_sec ASC) AS next_duration_sec_rank,
        MIN(duration_sec) OVER w_artist_id AS artist_id_min_duration_sec,
        LEAD(bpm_sum_7, 2) OVER w_release_date_year_duration_s AS next2_bpm_sum_7
    FROM windowed
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_release_date_year_duration_s AS (PARTITION BY YEAR(release_date), duration_sec_decile ORDER BY duration_sec_decile DESC, id)
)
SELECT
    id,
    title,
    duration_sec,
    bpm,
    seq_no,
    duration_sec_percent_rank,
    next_duration_sec,
    bpm_avg_3_prior,
    distinct_artist_id_in_is_explicit,
    duration_sec_vs_is_explicit_avg,
    max_bpm_so_far,
    is_explicit_max_duration_sec,
    duration_sec_decile,
    first_duration_sec,
    next2_release_date,
    next_duration_sec_rank,
    artist_id_min_duration_sec,
    next2_bpm_sum_7,
    LEAST(next_duration_sec, next2_bpm_sum_7) AS least_next_duration_sec_next2_bpm_sum_7,
    next2_bpm_sum_7 - next_duration_sec_rank AS next2_bpm_sum_7_minus_next_duration_sec_rank
FROM metrics
WHERE is_explicit_max_duration_sec < 2556
ORDER BY duration_sec_decile
