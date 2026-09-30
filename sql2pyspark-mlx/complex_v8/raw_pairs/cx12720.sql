WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref
    FROM tracks
    WHERE artist_id IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        title_ref,
        DATEDIFF(release_date, LAG(release_date) OVER w_artist_id_by_release_date) AS days_since_prev_release_date,
        LEAD(release_date, 2) OVER (PARTITION BY artist_id ORDER BY bpm, id) AS next2_release_date,
        ROUND(AVG(bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_3_prior,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit), 0), 4) AS bpm_share_of_is_explicit,
        LAG(bpm) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id) AS prev_bpm,
        SIZE(COLLECT_SET(artist_id) OVER w_is_explicit_release_date_yea) AS distinct_artist_id_in_is_explicit_release_date_y,
        LAST_VALUE(bpm) OVER (ORDER BY bpm NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        FIRST_VALUE(release_date) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        LAST_VALUE(release_date) OVER (PARTITION BY artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date,
        LAG(bpm, 2) OVER w_artist_id_by_release_date_2 AS prev2_bpm,
        COUNT(*) OVER (ORDER BY bpm NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(bpm) OVER (PARTITION BY artist_id, is_explicit) AS artist_id_is_explicit_max_bpm,
        SUM(bpm) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS bpm_sum_1_prior,
        ROUND(PERCENT_RANK() OVER w_all_by_bpm_ties, 3) AS bpm_percent_rank,
        SUM(bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS bpm_sum_3_prior
    FROM filtered
    WINDOW w_artist_id_by_release_date AS (PARTITION BY artist_id ORDER BY release_date, id), w_is_explicit_release_date_yea AS (PARTITION BY is_explicit, YEAR(release_date)), w_artist_id_by_release_date_2 AS (PARTITION BY artist_id ORDER BY release_date DESC, id), w_all_by_bpm_ties AS (ORDER BY bpm NULLS LAST)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(next2_release_date, 'yyyy-MM') AS next2_release_date_ym,
        DATE_FORMAT(last_release_date, 'yyyy-MM') AS last_release_date_ym,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt,
        SUM(last_bpm) AS total_last_bpm,
        ROUND(SUM(last_bpm) / NULLIF(SUM(running_count), 0), 3) AS last_bpm_to_running_count_ratio
    FROM with_trends
    GROUP BY DATE_FORMAT(next2_release_date, 'yyyy-MM'), DATE_FORMAT(last_release_date, 'yyyy-MM')
    HAVING AVG(last_bpm) > 76
)
SELECT
    next2_release_date_ym,
    last_release_date_ym,
    cnt,
    total_last_bpm,
    last_bpm_to_running_count_ratio,
    LEAST(total_last_bpm, cnt) AS least_total_last_bpm_cnt
FROM per_group
ORDER BY last_bpm_to_running_count_ratio DESC
