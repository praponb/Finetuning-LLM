WITH base_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text,
        bpm - duration_sec AS bpm_minus_duration_sec
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'focus')
      AND LOWER(title) LIKE '%lo%'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods_text,
        bpm_minus_duration_sec,
        ROUND(AVG(duration_sec) OVER (ORDER BY bpm_minus_duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_3_prior,
        ROUND(CUME_DIST() OVER w_all_by_bpm_minus_duration_se_2, 3) AS bpm_minus_duration_sec_cume_dist,
        SUM(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_sum_duration_sec,
        ROUND(AVG(bpm_minus_duration_sec) OVER (ORDER BY bpm_minus_duration_sec ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS bpm_minus_duration_sec_avg_7,
        ROUND(CUME_DIST() OVER (ORDER BY bpm_minus_duration_sec ASC, id), 3) AS bpm_minus_duration_sec_cume_dist_2,
        ROUND(CUME_DIST() OVER (ORDER BY bpm_minus_duration_sec DESC, id), 3) AS bpm_minus_duration_sec_cume_dist_3,
        LEAD(duration_sec, 2, 0) OVER (ORDER BY bpm_minus_duration_sec DESC, id) AS next2_duration_sec,
        ROUND(PERCENT_RANK() OVER (ORDER BY bpm_minus_duration_sec DESC), 3) AS bpm_minus_duration_sec_percent_rank,
        COUNT(*) OVER (ORDER BY bpm_minus_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(duration_sec) OVER (ORDER BY bpm_minus_duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_sum_4,
        MAX(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec,
        ROUND(bpm_minus_duration_sec / NULLIF(SUM(bpm_minus_duration_sec) OVER (PARTITION BY artist_id), 0), 4) AS bpm_minus_duration_sec_share_of_artist_id,
        ROUND(AVG(duration_sec) OVER (ORDER BY bpm_minus_duration_sec DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_2_prior,
        LEAD(bpm_minus_duration_sec) OVER (ORDER BY bpm_minus_duration_sec ASC, id) AS next_bpm_minus_duration_sec
    FROM base_tracks
    WINDOW w_all_by_bpm_minus_duration_se_2 AS (ORDER BY bpm_minus_duration_sec DESC)
),
stats AS (
    -- Summarise per group
    SELECT
        is_explicit,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        ROUND(SUM(duration_sec_avg_3_prior), 2) AS total_duration_sec_avg_3_prior,
        PERCENTILE_APPROX(next_bpm_minus_duration_sec, 0.5) AS median_next_bpm_minus_duration_sec,
        MAX(release_date) AS last_release_date,
        ROUND(SUM(bpm_minus_duration_sec_cume_dist_3), 2) AS total_bpm_minus_duration_sec_cume_dist_3,
        SUM(CASE WHEN UPPER(TRIM(title)) LIKE 'G%' THEN duration_sec_avg_2_prior ELSE 0 END) AS duration_sec_avg_2_prior_upper_trim_title_like_g,
        PERCENTILE_APPROX(bpm_minus_duration_sec_percent_rank, 0.5) AS median_bpm_minus_duration_sec_percent_rank
    FROM ranked
    GROUP BY is_explicit, DATE_TRUNC('MONTH', release_date)
)
-- Final projection
SELECT
    is_explicit,
    last_release_date,
    median_bpm_minus_duration_sec_percent_rank,
    DAYOFWEEK(release_date_month) AS release_date_month_dayofweek,
    LAST_DAY(last_release_date) AS last_release_date_month_end
FROM stats
ORDER BY median_bpm_minus_duration_sec_percent_rank, release_date_month_dayofweek
