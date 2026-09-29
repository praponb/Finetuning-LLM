WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        ABS(bpm - 94) AS bpm_distance,
        INITCAP(TRIM(title)) AS title_clean
    FROM tracks
    WHERE duration_sec >= 154
      AND UPPER(TRIM(title)) LIKE 'N%'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(release_date, 2) OVER (ORDER BY bpm_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        FIRST_VALUE(bpm_distance) OVER (ORDER BY bpm_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm_distance,
        MAX(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit ORDER BY bpm_distance DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS duration_sec_max_1_prior,
        NTH_VALUE(bpm_distance, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit ORDER BY bpm_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm_distance,
        SUM(bpm_distance) OVER (ORDER BY bpm_distance DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_distance_sum_6_prior,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit
    FROM base
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(second_bpm_distance) OVER (ORDER BY second_bpm_distance, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS second_bpm_distance_sum_7,
        ROW_NUMBER() OVER (ORDER BY second_release_date, id) AS rn,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        LEAD(bpm_distance, 3, 0) OVER (ORDER BY second_bpm_distance, id) AS next3_bpm_distance,
        ROUND(AVG(duration_sec) OVER (ORDER BY second_release_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_3
    FROM with_windows
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM ranked s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE t.bpm < 166
        )
)
SELECT id, metric, metric_value
FROM (
    SELECT id, rn, rows_in_artist_id, second_bpm_distance_sum_7, duration_sec
    FROM qualified
) src
UNPIVOT (
    metric_value FOR metric IN (rn, rows_in_artist_id, second_bpm_distance_sum_7, duration_sec)
) unp
