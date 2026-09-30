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
        moods
    FROM tracks
    WHERE YEAR(release_date) = 2024
      AND is_explicit = TRUE
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id, is_explicit), 2) AS bpm_vs_artist_id_is_explicit_avg,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id_is_explicit, 2) AS duration_sec_vs_artist_id_is_explicit_avg,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec) AS duration_sec_dense_rank,
        SUM(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_sum_duration_sec,
        ROUND(AVG(duration_sec) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_4,
        ROW_NUMBER() OVER (ORDER BY duration_sec DESC, id) AS seq_no,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec DESC, id), 3) AS duration_sec_percent_rank,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id,
        DATEDIFF(release_date, LAG(release_date) OVER w_artist_id_by_duration_sec) AS days_since_prev_release_date
    FROM valid
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_artist_id AS (PARTITION BY artist_id), w_artist_id_by_duration_sec AS (PARTITION BY artist_id ORDER BY duration_sec, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE duration_sec_dense_rank <= 5
      AND duration_sec_percent_rank < 0.7
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec_vs_artist_id_is_explicit_avg DESC NULLS FIRST, id) AS rn,
        ROUND(AVG(days_since_prev_release_date) OVER (PARTITION BY artist_id ORDER BY duration_sec_avg_4 DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS days_since_prev_release_date_avg_2,
        MAX(duration_sec_avg_4) OVER (ORDER BY artist_id_sum_duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_avg_4_so_far,
        ROUND(AVG(duration_sec_percent_rank) OVER (PARTITION BY is_explicit), 2) AS is_explicit_avg_duration_sec_percent_rank,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY is_explicit), 2) AS duration_sec_vs_is_explicit_avg,
        FIRST_VALUE(bpm) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec_vs_artist_id_is_explicit_avg DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit,
        LAST_VALUE(rows_in_artist_id) OVER (PARTITION BY artist_id ORDER BY duration_sec_avg_4 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rows_in_artist_id
    FROM first_rows
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        seq_no,
        is_explicit,
        CASE WHEN bpm >= 165 THEN 'large' WHEN bpm >= 100 THEN 'medium' ELSE 'small' END AS bpm_tier,
        COUNT(*) AS n_rows,
        ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(rn) AS min_rn
    FROM sequenced
    GROUP BY seq_no, is_explicit, CASE WHEN bpm >= 165 THEN 'large' WHEN bpm >= 100 THEN 'medium' ELSE 'small' END
)
-- Final projection
SELECT
    seq_no,
    is_explicit,
    stddev_duration_sec,
    distinct_ids,
    min_rn,
    COALESCE(seq_no, 0) AS seq_no_filled,
    COALESCE(seq_no, 0) AS seq_no_filled_2
FROM per_group
WHERE bpm_tier IN ('medium', 'large')
