WITH cleaned_tracks AS (
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
        CASE WHEN bpm >= 142 THEN 'HIGH' WHEN bpm >= 89 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier,
        duration_sec + bpm AS duration_sec_plus_bpm,
        MONTH(release_date) AS release_date_month
    FROM tracks
    WHERE artist_id <= 5
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month_2,
        artist_id,
        is_explicit,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS row_count,
        MAX(duration_sec) AS max_duration_sec,
        MIN(bpm) AS min_bpm
    FROM cleaned_tracks
    WHERE duration_sec_plus_bpm >= 303
    GROUP BY DATE_TRUNC('MONTH', release_date), artist_id, is_explicit
    HAVING COUNT(*) >= 3
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(distinct_ids - 6) AS distinct_ids_distance,
        DATE_TRUNC('YEAR', release_date_month_2) AS release_date_month_2_year
    FROM grouped
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(max_duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY row_count ASC, release_date_month_2, artist_id, is_explicit ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS max_duration_sec_avg_2,
        MAX(max_duration_sec) OVER (PARTITION BY distinct_ids_distance ORDER BY max_duration_sec, release_date_month_2, artist_id, is_explicit ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS max_duration_sec_max_3,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_ids_distance ORDER BY max_duration_sec), 3) AS max_duration_sec_cume_dist,
        NTH_VALUE(min_bpm, 2) OVER (PARTITION BY artist_id, is_explicit ORDER BY row_count ASC, release_date_month_2, artist_id, is_explicit ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_bpm,
        SUM(max_duration_sec) OVER (PARTITION BY MONTH(release_date_month_2)) AS release_date_month_2_month_sum_max_duration_sec
    FROM annotated
)
-- Final projection
SELECT
    release_date_month_2,
    artist_id,
    row_count,
    min_bpm,
    distinct_ids_distance,
    release_date_month_2_year,
    max_duration_sec_max_3,
    max_duration_sec_cume_dist,
    second_min_bpm,
    release_date_month_2_month_sum_max_duration_sec,
    ABS(distinct_ids_distance - 4) AS distinct_ids_distance_distance
FROM ranked
ORDER BY distinct_ids_distance_distance NULLS LAST, release_date_month_2_year DESC
