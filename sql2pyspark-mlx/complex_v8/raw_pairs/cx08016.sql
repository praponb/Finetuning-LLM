WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        moods
    FROM tracks
    WHERE (bpm > 85 OR duration_sec < 312)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY release_date), 3) AS release_date_cume_dist,
        ROUND(AVG(duration_sec) OVER w_release_date_month_artist_id, 2) AS release_date_month_artist_id_avg_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id ORDER BY release_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_sum_3,
        LEAD(duration_sec, 1, 0) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY release_date DESC, id) AS next_duration_sec,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        NTILE(5) OVER (ORDER BY duration_sec DESC, id) AS duration_sec_quintile,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY release_date, id)) AS days_since_prev_release_date,
        MIN(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_min_duration_sec,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id, 0), 4) AS duration_sec_share_of_artist_id,
        MAX(duration_sec) OVER w_release_date_month_artist_id AS release_date_month_artist_id_max_duration_sec,
        NTH_VALUE(release_date, 2) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        MAX(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id ORDER BY release_date, id) AS release_date_quintile,
        NTILE(3) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id) AS duration_sec_tercile,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec
    FROM prepared
    WINDOW w_release_date_month_artist_id AS (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id), w_artist_id AS (PARTITION BY artist_id)
),
per_group AS (
    -- Group-level totals
    SELECT
        release_date_quintile,
        DATE_TRUNC('MONTH', second_release_date) AS second_release_date_month,
        duration_sec_tercile,
        ROUND(AVG(artist_id_min_duration_sec), 2) AS avg_artist_id_min_duration_sec,
        PERCENTILE_APPROX(release_date_quintile, 0.5) AS median_release_date_quintile,
        MAX(duration_sec_sum_3) AS max_duration_sec_sum_3,
        PERCENTILE_APPROX(release_date_month_artist_id_max_duration_sec, 0.5) AS median_release_date_month_artist_id_max_duration,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(release_date) AS last_release_date
    FROM windowed
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    release_date_quintile,
    second_release_date_month,
    duration_sec_tercile,
    avg_artist_id_min_duration_sec,
    median_release_date_quintile,
    median_release_date_month_artist_id_max_duration,
    last_release_date,
    max_duration_sec_sum_3 - distinct_ids AS max_duration_sec_sum_3_minus_distinct_ids,
    ROUND(release_date_quintile / NULLIF(max_duration_sec_sum_3, 0), 3) AS release_date_quintile_per_max_duration_sec_sum_3
FROM per_group
WHERE median_release_date_quintile >= 2
ORDER BY median_release_date_quintile DESC, duration_sec_tercile NULLS LAST, release_date_quintile, second_release_date_month
LIMIT 20
