WITH cleaned_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-09-26' AND DATE '2025-08-07'
      AND artist_id >= 11
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY is_explicit) AS is_explicit_sum_duration_sec,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        duration_sec - LAG(duration_sec) OVER w_all_by_duration_sec AS duration_sec_change,
        MIN(bpm) OVER (PARTITION BY MONTH(release_date), artist_id) AS release_date_month_artist_id_min_bpm,
        MAX(duration_sec) OVER (PARTITION BY artist_id ORDER BY release_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_max_6_prior,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit), 2) AS is_explicit_avg_bpm,
        MAX(bpm) OVER (PARTITION BY MONTH(release_date), artist_id) AS release_date_month_artist_id_max_bpm,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY artist_id ORDER BY release_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec
    FROM cleaned_tracks
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LAST_DAY(release_date) AS release_date_month_end,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        ADD_MONTHS(release_date, 6) AS release_date_plus_6m
    FROM windowed
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(bpm) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date_plus_6m) ORDER BY release_date_month_end, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS bpm_sum_1_prior,
        LEAD(second_bpm) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date_plus_6m) ORDER BY release_date_month_end, id) AS next_second_bpm,
        SUM(is_explicit_sum_duration_sec) OVER (PARTITION BY MONTH(release_date_plus_6m) ORDER BY second_bpm, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS is_explicit_sum_duration_sec_sum_1_prior,
        ROUND(AVG(second_duration_sec) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date_plus_6m) ORDER BY release_date_month_end, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS second_duration_sec_avg_4,
        LAST_VALUE(release_date_month_end) IGNORE NULLS OVER (ORDER BY second_bpm ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date_month_end,
        ROUND(PERCENT_RANK() OVER w_is_explicit_release_date_plu_4, 3) AS release_date_month_end_percent_rank
    FROM scored
    WINDOW w_is_explicit_release_date_plu_4 AS (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date_plus_6m) ORDER BY release_date_month_end)
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', release_date_month_end) AS release_date_month_end_month,
        is_explicit,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        COUNT(*) FILTER (WHERE LENGTH(TRIM(title)) >= 7) AS n_length_trim_title_7,
        MIN(release_date_month_end) AS first_release_date_month_end,
        COUNT(*) AS cnt
    FROM with_trends
    GROUP BY DATE_TRUNC('MONTH', release_date_month_end), is_explicit
)
SELECT
    release_date_month_end_month,
    is_explicit,
    title_list,
    n_length_trim_title_7,
    first_release_date_month_end,
    cnt,
    ROUND(n_length_trim_title_7 / NULLIF(cnt, 0), 3) AS n_length_trim_title_7_per_cnt,
    FLOOR(n_length_trim_title_7 / 2) * 2 AS n_length_trim_title_7_bucket
FROM summary
WHERE first_release_date_month_end >= DATE '2024-07-01'
ORDER BY n_length_trim_title_7, n_length_trim_title_7_per_cnt
