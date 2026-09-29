WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        DATE_TRUNC('YEAR', release_date) AS release_date_year,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE duration_sec >= 183
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY TO_DATE(release_date_year)), 0), 4) AS duration_sec_share_of_release_date_year_to_date,
        LAG(bpm, 3) OVER w_artist_id_title_len_by_bpm AS prev3_bpm,
        ROUND(AVG(duration_sec) OVER (ORDER BY title_len, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_3,
        ROW_NUMBER() OVER (ORDER BY title_len, id) AS rn,
        SIZE(COLLECT_SET(artist_id) OVER w_title_len) AS distinct_artist_id_in_title_len,
        ROW_NUMBER() OVER (PARTITION BY artist_id, title_len ORDER BY bpm ASC, id) AS bpm_seq,
        ROUND(CUME_DIST() OVER (ORDER BY title_len), 3) AS title_len_cume_dist,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY TO_DATE(release_date_year))) AS distinct_artist_id_in_release_date_year_to_date,
        ROUND(title_len - AVG(title_len) OVER w_title_len, 2) AS title_len_vs_title_len_avg,
        MAX(title_len) OVER (ORDER BY title_len, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS title_len_max_3_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, title_len ORDER BY bpm ASC), 3) AS bpm_percent_rank,
        RANK() OVER (PARTITION BY TO_DATE(release_date_year), title_len ORDER BY release_date_year ASC NULLS LAST) AS release_date_year_rank,
        DENSE_RANK() OVER w_all_by_duration_sec_ties AS duration_sec_dense_rank,
        ROUND(AVG(duration_sec) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_3_2,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id, title_len), 0), 4) AS bpm_share_of_artist_id_title_len
    FROM scoped_tracks
    WINDOW w_artist_id_title_len_by_bpm AS (PARTITION BY artist_id, title_len ORDER BY bpm ASC, id), w_title_len AS (PARTITION BY title_len), w_all_by_duration_sec_ties AS (ORDER BY duration_sec DESC)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        title_len_max_3_prior - LAG(title_len_max_3_prior) OVER (ORDER BY bpm_share_of_artist_id_title_len, id) AS title_len_max_3_prior_change,
        release_date_year_rank - LAG(release_date_year_rank) OVER w_artist_id_by_distinct_artist AS release_date_year_rank_change,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_distinct_artist, 3) AS distinct_artist_id_in_title_len_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY artist_id, release_date_year_rank ORDER BY title_len_max_3_prior DESC NULLS FIRST, id) AS title_len_max_3_prior_seq,
        ROUND(AVG(title_len_cume_dist) OVER w_title_len, 2) AS title_len_avg_title_len_cume_dist,
        COUNT(*) OVER (ORDER BY bpm_share_of_artist_id_title_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(title_len_vs_title_len_avg / NULLIF(SUM(title_len_vs_title_len_avg) OVER (PARTITION BY title_len), 0), 4) AS title_len_vs_title_len_avg_share_of_title_len,
        ROUND(title_len_vs_title_len_avg - AVG(title_len_vs_title_len_avg) OVER (PARTITION BY title_len), 2) AS title_len_vs_title_len_avg_vs_title_len_avg
    FROM with_windows
    WINDOW w_artist_id_by_distinct_artist AS (PARTITION BY artist_id ORDER BY distinct_artist_id_in_title_len DESC, id), w_title_len AS (PARTITION BY title_len)
)
-- Final projection
SELECT DISTINCT
    title_len_avg_title_len_cume_dist,
    title_len_vs_title_len_avg_share_of_title_len
FROM metrics
ORDER BY title_len_vs_title_len_avg_share_of_title_len DESC, title_len_avg_title_len_cume_dist DESC
