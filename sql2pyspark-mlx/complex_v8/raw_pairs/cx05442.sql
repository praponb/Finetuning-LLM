WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        GREATEST(duration_sec, bpm) AS greatest_duration_sec_bpm,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'G%'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(greatest_duration_sec_bpm) OVER (PARTITION BY artist_id, is_explicit ORDER BY greatest_duration_sec_bpm NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS greatest_duration_sec_bpm_max_6_prior,
        ROUND(greatest_duration_sec_bpm / NULLIF(SUM(greatest_duration_sec_bpm) OVER w_release_date_month_month_art, 0), 4) AS greatest_duration_sec_bpm_share_of_release_date_,
        ROUND(AVG(greatest_duration_sec_bpm) OVER (PARTITION BY YEAR(release_date), is_explicit), 2) AS release_date_year_is_explicit_avg_greatest_durat,
        FIRST_VALUE(release_date_month) IGNORE NULLS OVER (PARTITION BY MONTH(release_date_month), is_explicit ORDER BY release_date_month NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date_month,
        ROUND(AVG(greatest_duration_sec_bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date_month), artist_id), 2) AS release_date_month_month_artist_id_avg_greatest_,
        ROUND(AVG(greatest_duration_sec_bpm) OVER (PARTITION BY artist_id, is_explicit ORDER BY greatest_duration_sec_bpm NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS greatest_duration_sec_bpm_avg_3,
        COUNT(*) OVER w_release_date_month_month_art AS rows_in_release_date_month_month_artist_id,
        NTH_VALUE(release_date_month, 2) OVER (PARTITION BY MONTH(release_date_month), is_explicit ORDER BY release_date_month NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date_month,
        NTILE(10) OVER w_is_explicit_by_release_date AS release_date_decile,
        ROUND(greatest_duration_sec_bpm - AVG(greatest_duration_sec_bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit), 2) AS greatest_duration_sec_bpm_vs_release_date_month_,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit)) AS distinct_artist_id_in_release_date_month_is_expl,
        ROUND(AVG(greatest_duration_sec_bpm) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS greatest_duration_sec_bpm_avg_4,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id)) AS days_since_prev_release_date
    FROM filtered
    WINDOW w_release_date_month_month_art AS (PARTITION BY DATE_TRUNC('MONTH', release_date_month), artist_id), w_is_explicit_by_release_date AS (PARTITION BY is_explicit ORDER BY release_date DESC, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        release_date_month,
        greatest_duration_sec_bpm_max_6_prior,
        greatest_duration_sec_bpm_share_of_release_date_,
        release_date_year_is_explicit_avg_greatest_durat,
        first_known_release_date_month,
        release_date_month_month_artist_id_avg_greatest_,
        greatest_duration_sec_bpm_avg_3,
        second_release_date_month,
        greatest_duration_sec_bpm_vs_release_date_month_,
        distinct_artist_id_in_release_date_month_is_expl,
        greatest_duration_sec_bpm_avg_4,
        ROUND(AVG(days_since_prev_release_date) OVER (PARTITION BY TO_DATE(second_release_date_month), is_explicit), 2) AS second_release_date_month_to_date_is_explicit_av,
        LAG(release_date_decile, 3) OVER (ORDER BY release_date_month, id) AS prev3_release_date_decile,
        DENSE_RANK() OVER (PARTITION BY artist_id, YEAR(release_date_month) ORDER BY days_since_prev_release_date ASC, id) AS days_since_prev_release_date_dense_rank,
        LAST_VALUE(second_release_date_month) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), distinct_artist_id_in_release_date_month_is_expl ORDER BY release_date_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_second_release_date_month,
        DENSE_RANK() OVER w_artist_id_release_date_month_2 AS days_since_prev_release_date_dense_rank_2,
        NTILE(5) OVER (ORDER BY release_date_month, id) AS release_date_month_quintile
    FROM with_windows
    WINDOW w_artist_id_release_date_month_2 AS (PARTITION BY artist_id, YEAR(release_date_month) ORDER BY days_since_prev_release_date ASC)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        release_date_month_quintile,
        TO_DATE(first_known_release_date_month) AS first_known_release_date_month_day,
        ROUND(100 * COUNT(CASE WHEN NOT is_explicit THEN 1 END) / COUNT(*), 1) AS pct_not_is_explicit,
        SUM(prev3_release_date_decile) FILTER (WHERE is_explicit = FALSE) AS prev3_release_date_decile_is_explicit_false,
        MIN(greatest_duration_sec_bpm_max_6_prior) AS min_greatest_duration_sec_bpm_max_6_prior,
        ROUND(SUM(release_date_month_month_artist_id_avg_greatest_), 2) AS total_release_date_month_month_artist_id_avg_gre,
        SUM(CASE WHEN NOT is_explicit THEN 1 ELSE 0 END) AS n_not_is_explicit
    FROM metrics
    GROUP BY release_date_month_quintile, TO_DATE(first_known_release_date_month)
    HAVING AVG(distinct_artist_id_in_release_date_month_is_expl) > 2
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(n_not_is_explicit) OVER (PARTITION BY n_not_is_explicit, release_date_month_quintile ORDER BY min_greatest_duration_sec_bpm_max_6_prior DESC, release_date_month_quintile, first_known_release_date_month_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_n_not_is_explicit,
        MIN(release_date_month_quintile) OVER (PARTITION BY n_not_is_explicit, release_date_month_quintile ORDER BY min_greatest_duration_sec_bpm_max_6_prior DESC, release_date_month_quintile, first_known_release_date_month_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_release_date_month_quintile_so_far,
        RANK() OVER (PARTITION BY n_not_is_explicit, release_date_month_quintile ORDER BY min_greatest_duration_sec_bpm_max_6_prior DESC, release_date_month_quintile, first_known_release_date_month_day) AS min_greatest_duration_sec_bpm_max_6_prior_rank,
        pct_not_is_explicit - LAG(pct_not_is_explicit) OVER w_n_not_is_explicit_release_da_2 AS pct_not_is_explicit_change
    FROM rollup_base
    WINDOW w_n_not_is_explicit_release_da_2 AS (PARTITION BY n_not_is_explicit, release_date_month_quintile ORDER BY min_greatest_duration_sec_bpm_max_6_prior DESC, release_date_month_quintile, first_known_release_date_month_day)
)
SELECT DISTINCT
    release_date_month_quintile,
    min_greatest_duration_sec_bpm_max_6_prior_rank,
    pct_not_is_explicit_change
FROM with_windows_2
WHERE running_n_not_is_explicit > 25
ORDER BY pct_not_is_explicit_change DESC
