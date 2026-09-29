WITH valid AS (
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
        LOWER(TRIM(title)) AS title_clean,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE (bpm BETWEEN 73 AND 143 OR release_date >= DATE '2024-07-01')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(bpm_per_duration_sec) OVER (PARTITION BY MONTH(release_date) ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm_per_duration_sec,
        NTH_VALUE(bpm_per_duration_sec, 2) OVER (PARTITION BY is_explicit ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm_per_duration_sec,
        duration_sec - LAG(duration_sec) OVER w_is_explicit_by_release_date_2 AS duration_sec_change,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC NULLS LAST), 3) AS release_date_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY release_date), 3) AS release_date_cume_dist_2,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        LAST_VALUE(release_date) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date,
        SUM(bpm_per_duration_sec) OVER (PARTITION BY is_explicit ORDER BY release_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS bpm_per_duration_sec_sum_3_prior,
        ROUND(AVG(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_1_prior,
        RANK() OVER w_release_date_month_by_bpm_ti AS bpm_rank
    FROM valid
    WINDOW w_is_explicit_by_release_date_2 AS (PARTITION BY is_explicit ORDER BY release_date, id), w_release_date_month_by_bpm_ti AS (PARTITION BY MONTH(release_date) ORDER BY bpm DESC)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(release_date) AS release_date_year,
        DATE_FORMAT(last_release_date, 'yyyy-MM') AS last_release_date_ym,
        CASE WHEN bpm >= 142 THEN 'large' WHEN bpm >= 85 THEN 'medium' ELSE 'small' END AS bpm_tier,
        ROUND(AVG(bpm), 2) AS avg_bpm,
        COUNT(*) AS row_count
    FROM ranked
    WHERE artist_id >= 3
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    release_date_year,
    bpm_tier,
    avg_bpm,
    row_count,
    FLOOR(row_count / 2) * 2 AS row_count_bucket
FROM grouped
WHERE last_release_date_ym IS NOT NULL
ORDER BY row_count_bucket
