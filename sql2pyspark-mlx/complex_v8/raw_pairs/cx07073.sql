WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'workout')
      AND duration_sec < 352
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        is_explicit_flag,
        title_prefix,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY is_explicit_flag, artist_id), 0), 4) AS duration_sec_share_of_is_explicit_flag_artist_id
    FROM valid
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        is_explicit_flag,
        MAX(duration_sec) AS max_duration_sec,
        DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days,
        SUM(CASE WHEN title IS NOT NULL THEN is_explicit_flag ELSE 0 END) AS is_explicit_flag_title_is_not_null,
        ROUND(STDDEV(is_explicit_flag), 2) AS stddev_is_explicit_flag,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids
    FROM metrics
    WHERE UPPER(TRIM(title)) LIKE 'G%'
    GROUP BY DATE_FORMAT(release_date, 'yyyy-MM'), is_explicit_flag
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE stddev_is_explicit_flag > (SELECT AVG(stddev_is_explicit_flag) FROM rollup_base)
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(is_explicit_flag - AVG(is_explicit_flag) OVER w_is_explicit_flag, 2) AS is_explicit_flag_vs_is_explicit_flag_avg,
        ROUND(release_date_span_days / NULLIF(SUM(release_date_span_days) OVER w_is_explicit_flag, 0), 4) AS release_date_span_days_share_of_is_explicit_flag,
        DENSE_RANK() OVER (ORDER BY max_duration_sec, release_date_ym, is_explicit_flag) AS max_duration_sec_dense_rank,
        ROUND(max_duration_sec - AVG(max_duration_sec) OVER w_is_explicit_flag, 2) AS max_duration_sec_vs_is_explicit_flag_avg,
        ROUND(AVG(is_explicit_flag) OVER (PARTITION BY is_explicit_flag), 2) AS is_explicit_flag_avg_is_explicit_flag,
        NTH_VALUE(is_explicit_flag, 2) OVER (ORDER BY max_duration_sec, release_date_ym, is_explicit_flag ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_is_explicit_flag
    FROM with_activity
    WINDOW w_is_explicit_flag AS (PARTITION BY is_explicit_flag)
)
-- Final projection
SELECT
    release_date_ym,
    is_explicit_flag,
    max_duration_sec,
    release_date_span_days,
    is_explicit_flag_title_is_not_null,
    stddev_is_explicit_flag,
    is_explicit_flag_vs_is_explicit_flag_avg,
    release_date_span_days_share_of_is_explicit_flag,
    max_duration_sec_dense_rank,
    second_is_explicit_flag,
    ROUND(release_date_span_days / NULLIF(is_explicit_flag, 0), 3) AS release_date_span_days_per_is_explicit_flag
FROM metrics_2
ORDER BY is_explicit_flag_vs_is_explicit_flag_avg, max_duration_sec_dense_rank NULLS LAST
