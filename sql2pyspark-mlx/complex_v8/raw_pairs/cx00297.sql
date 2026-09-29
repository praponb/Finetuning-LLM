WITH prepared_tracks AS (
    -- Filter tracks
    SELECT *
    FROM tracks
    WHERE duration_sec BETWEEN 165 AND 402
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        ROUND(AVG(duration_sec) OVER w_artist_id, 2) AS artist_id_avg_duration_sec,
        DATEDIFF(release_date, LAG(release_date) OVER w_artist_id_release_date_year) AS days_since_prev_release_date,
        duration_sec - LAG(duration_sec) OVER w_all_by_release_date AS duration_sec_change,
        ROUND(AVG(duration_sec) OVER (ORDER BY release_date NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_3_prior,
        MAX(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY artist_id, YEAR(release_date) ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        bpm - LAG(bpm) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id) AS bpm_change,
        ROUND(bpm - AVG(bpm) OVER w_artist_id, 2) AS bpm_vs_artist_id_avg,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, YEAR(release_date) ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_4
    FROM prepared_tracks
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_artist_id_release_date_year AS (PARTITION BY artist_id, YEAR(release_date) ORDER BY duration_sec, id), w_all_by_release_date AS (ORDER BY release_date NULLS LAST, id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(rows_in_artist_id, 0) AS rows_in_artist_id_filled,
        CASE WHEN bpm_vs_artist_id_avg >= 36.0 THEN 'HIGH' WHEN bpm_vs_artist_id_avg >= -36.0 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_vs_artist_id_avg_tier,
        LENGTH(TRIM(title)) AS title_len
    FROM ranked
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        is_explicit,
        SUM(CASE WHEN UPPER(TRIM(title)) LIKE 'R%' THEN 1 ELSE 0 END) AS n_upper_trim_title_like_r,
        COUNT(*) AS cnt,
        ROUND(SUM(artist_id_avg_duration_sec) / NULLIF(SUM(second_duration_sec), 0), 3) AS artist_id_avg_duration_sec_to_second_duration_se,
        SUM(title_len) AS total_title_len,
        ROUND(AVG(duration_sec_change), 2) AS avg_duration_sec_change
    FROM labeled
    GROUP BY DATE_TRUNC('MONTH', release_date), is_explicit
)
-- Final projection
SELECT
    release_date_month,
    n_upper_trim_title_like_r,
    artist_id_avg_duration_sec_to_second_duration_se,
    avg_duration_sec_change,
    COALESCE(avg_duration_sec_change, 0.0) AS avg_duration_sec_change_filled
FROM grouped
ORDER BY artist_id_avg_duration_sec_to_second_duration_se, release_date_month DESC
