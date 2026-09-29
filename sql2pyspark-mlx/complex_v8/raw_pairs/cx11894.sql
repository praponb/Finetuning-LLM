WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        moods,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE artist_id IS NOT NULL
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids
    FROM valid_tracks
    GROUP BY 1
    HAVING MAX(bpm_bucket) > 108
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY TO_DATE(release_date_month), row_count ORDER BY distinct_ids) AS distinct_ids_rank,
        MAX(distinct_ids) OVER (ORDER BY row_count, release_date_month ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS distinct_ids_max_7,
        COUNT(*) OVER w_release_date_month_month_row AS rows_in_release_date_month_month_row_count,
        LEAD(release_date_month, 2) OVER (PARTITION BY TO_DATE(release_date_month), row_count ORDER BY distinct_ids, release_date_month) AS next2_release_date_month,
        LAG(release_date_month, 2) OVER (PARTITION BY TO_DATE(release_date_month), row_count ORDER BY distinct_ids, release_date_month) AS prev2_release_date_month,
        ROUND(PERCENT_RANK() OVER w_row_count_distinct_ids_by_di, 3) AS distinct_ids_percent_rank
    FROM per_group
    WINDOW w_release_date_month_month_row AS (PARTITION BY DATE_TRUNC('MONTH', release_date_month), row_count), w_row_count_distinct_ids_by_di AS (PARTITION BY row_count, distinct_ids ORDER BY distinct_ids DESC NULLS FIRST)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_windows
    WHERE distinct_ids_max_7 > (SELECT AVG(distinct_ids_max_7) FROM with_windows)
)
SELECT release_date_month, metric, metric_value
FROM (
    SELECT release_date_month, row_count, distinct_ids_rank, rows_in_release_date_month_month_row_count
    FROM with_activity
) src
UNPIVOT (
    metric_value FOR metric IN (row_count, distinct_ids_rank, rows_in_release_date_month_month_row_count)
) unp
