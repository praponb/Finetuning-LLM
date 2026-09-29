WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
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
    WHERE bpm >= 100
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_duration_sec,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        COUNT(*) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM filtered_tracks
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        SUM(first_known_bpm) AS total_first_known_bpm,
        MIN(release_date) AS first_release_date,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(running_count) AS total_running_count
    FROM sequenced
    WHERE is_explicit = FALSE
    GROUP BY 1
    HAVING AVG(running_count) > 5
),
derived AS (
    -- Add calculated columns
    SELECT
        release_date_ym,
        first_release_date,
        cnt,
        distinct_ids,
        total_running_count,
        LOWER(REGEXP_REPLACE(TRIM(release_date_ym), ' ', '_')) AS release_date_ym_slug
    FROM stats
)
-- Subtotals with ROLLUP
SELECT
    cnt,
    distinct_ids,
    GROUPING_ID() AS grouping_level,
    ROUND(AVG(distinct_ids), 2) AS avg_distinct_ids,
    MAX(cnt) AS max_cnt
FROM derived
GROUP BY ROLLUP(cnt, distinct_ids)
ORDER BY cnt NULLS LAST, distinct_ids NULLS LAST
