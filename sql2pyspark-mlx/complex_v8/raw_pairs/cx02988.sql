WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM tracks
    WHERE artist_id >= 8
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER w_artist_id_release_date_month AS bpm_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, YEAR(release_date) ORDER BY bpm), 3) AS bpm_percent_rank,
        NTILE(3) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY bpm DESC, id) AS bpm_tercile,
        DENSE_RANK() OVER (ORDER BY release_date, id) AS release_date_dense_rank,
        NTILE(4) OVER w_artist_id_release_date_month AS bpm_quartile,
        SUM(bpm) OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec ASC, id), 3) AS duration_sec_percent_rank,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id ORDER BY release_date ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_7,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date
    FROM prepared
    WINDOW w_artist_id_release_date_month AS (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY bpm DESC, id)
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        moods,
        bpm_decile,
        bpm_percent_rank,
        bpm_tercile,
        bpm_quartile,
        running_bpm,
        artist_id_avg_bpm,
        bpm_avg_7,
        second_bpm,
        second_release_date,
        DENSE_RANK() OVER w_artist_id_by_bpm_tercile AS bpm_tercile_dense_rank,
        SIZE(COLLECT_SET(artist_id) OVER w_bpm_decile_bpm_tercile) AS distinct_artist_id_in_bpm_decile_bpm_tercile,
        ROUND(duration_sec_percent_rank / NULLIF(SUM(duration_sec_percent_rank) OVER (PARTITION BY bpm_decile, bpm_tercile), 0), 4) AS duration_sec_percent_rank_share_of_bpm_decile_bp,
        LEAD(bpm_avg_7, 1, 0) OVER (PARTITION BY artist_id ORDER BY bpm_tercile DESC, id) AS next_bpm_avg_7
    FROM with_trends
    WINDOW w_artist_id_by_bpm_tercile AS (PARTITION BY artist_id ORDER BY bpm_tercile DESC, id), w_bpm_decile_bpm_tercile AS (PARTITION BY bpm_decile, bpm_tercile)
),
summary AS (
    -- Group-level totals
    SELECT
        bpm_quartile,
        bpm_tercile,
        MAX(release_date) AS last_release_date,
        MIN(release_date) AS first_release_date
    FROM with_trends_2
    WHERE second_release_date < DATE '2025-03-29'
    GROUP BY bpm_quartile, bpm_tercile
    HAVING MAX(bpm_percent_rank) > 0.3
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_last_release_date_tie, 3) AS last_release_date_cume_dist,
        SUM(bpm_tercile) OVER (ORDER BY last_release_date, bpm_quartile, bpm_tercile ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm_tercile,
        COUNT(*) OVER (ORDER BY last_release_date, bpm_quartile, bpm_tercile ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(bpm_tercile / NULLIF(SUM(bpm_tercile) OVER (PARTITION BY bpm_quartile, bpm_tercile), 0), 4) AS bpm_tercile_share_of_bpm_quartile_bpm_tercile
    FROM summary
    WINDOW w_all_by_last_release_date_tie AS (ORDER BY last_release_date)
)
SELECT
    bpm_quartile,
    bpm_tercile,
    last_release_date,
    first_release_date,
    last_release_date_cume_dist,
    running_bpm_tercile,
    bpm_tercile_share_of_bpm_quartile_bpm_tercile
FROM with_windows
WHERE last_release_date BETWEEN DATE '2024-05-12' AND DATE '2025-11-14'
ORDER BY running_bpm_tercile DESC, last_release_date
