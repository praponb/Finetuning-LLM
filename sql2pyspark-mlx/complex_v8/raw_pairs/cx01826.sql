WITH cleaned_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        CASE WHEN duration_sec >= 352 THEN 'large' WHEN duration_sec >= 183 THEN 'medium' ELSE 'small' END AS duration_sec_tier
    FROM tracks
    WHERE artist_id >= 10
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        release_date_label,
        duration_sec_tier,
        NTH_VALUE(release_date, 2) OVER (ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date, id) AS seq_no,
        LAST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY duration_sec_tier)) AS distinct_artist_id_in_duration_sec_tier,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY duration_sec_tier), 0), 4) AS bpm_share_of_duration_sec_tier,
        ROUND(PERCENT_RANK() OVER w_release_date_month_by_releas, 3) AS release_date_percent_rank,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY bpm) AS bpm_dense_rank,
        bpm - LAG(bpm) OVER w_artist_id_by_bpm AS bpm_change,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        ROUND(CUME_DIST() OVER (ORDER BY release_date ASC), 3) AS release_date_cume_dist
    FROM cleaned_tracks
    WINDOW w_release_date_month_by_releas AS (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date, id), w_artist_id_by_bpm AS (PARTITION BY artist_id ORDER BY bpm, id), w_duration_sec_tier AS (PARTITION BY duration_sec_tier)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE bpm_dense_rank = 1
      AND release_date_percent_rank >= 0.2
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(bpm_change) IGNORE NULLS OVER (PARTITION BY artist_id, seq_no ORDER BY release_date_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm_change,
        MIN(seq_no) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_release_date)) AS last_known_release_date_month_min_seq_no,
        bpm - LAG(bpm) OVER w_all_by_bpm AS bpm_change_2
    FROM leaders
    WINDOW w_all_by_bpm AS (ORDER BY bpm, id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT artist_id, COUNT(*) AS n_rows, SUM(bpm_change) AS total_bpm_change
    FROM windowed_2
    GROUP BY artist_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(total_bpm_change) OVER (PARTITION BY n_rows ORDER BY total_bpm_change NULLS LAST, artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_total_bpm_change,
        ROUND(AVG(n_rows) OVER (PARTITION BY n_rows ORDER BY total_bpm_change NULLS LAST, artist_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS n_rows_avg_2,
        ROUND(total_bpm_change - AVG(total_bpm_change) OVER w_artist_id_n_rows, 2) AS total_bpm_change_vs_artist_id_n_rows_avg
    FROM per_group
    WINDOW w_artist_id_n_rows AS (PARTITION BY artist_id, n_rows)
)
-- Final projection
SELECT
    artist_id,
    n_rows,
    total_bpm_change,
    last_total_bpm_change,
    n_rows_avg_2,
    total_bpm_change_vs_artist_id_n_rows_avg,
    COALESCE(n_rows, 0) AS n_rows_filled,
    FLOOR(total_bpm_change / 250) * 250 AS total_bpm_change_bucket
FROM with_trends
WHERE n_rows_avg_2 >= 4.8
ORDER BY n_rows_filled DESC, artist_id
LIMIT 20
