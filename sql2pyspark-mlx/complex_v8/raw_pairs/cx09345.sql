WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        SIZE(moods) AS moods_count,
        ADD_MONTHS(release_date, 1) AS release_date_plus_1m,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE SIZE(moods) >= 1
      AND release_date >= DATE '2024-11-06'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        bpm_per_duration_sec,
        ROW_NUMBER() OVER w_moods_count_by_bpm_per_durat AS rn,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY moods_count ORDER BY bpm_per_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        SUM(duration_sec) OVER (PARTITION BY moods_count ORDER BY bpm_per_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec
    FROM recent_tracks
    WINDOW w_moods_count_by_bpm_per_durat AS (PARTITION BY moods_count ORDER BY bpm_per_duration_sec, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, bpm, running_duration_sec
    FROM with_trends
) src
UNPIVOT (
    value FOR metric_name IN (bpm, running_duration_sec)
) unp
