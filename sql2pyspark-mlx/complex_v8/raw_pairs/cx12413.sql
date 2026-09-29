WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE (release_date >= DATE '2024-12-03' OR bpm >= 96)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec DESC NULLS FIRST, id), 3) AS duration_sec_percent_rank,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_duration_sec
    FROM recent
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY is_explicit ORDER BY duration_sec_percent_rank, id) AS rn,
        SUM(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec_percent_rank, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS duration_sec_sum_2,
        SUM(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec_percent_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_sum_3,
        NTILE(3) OVER w_is_explicit_by_duration_sec AS duration_sec_percent_rank_tercile
    FROM metrics
    WINDOW w_is_explicit_by_duration_sec AS (PARTITION BY is_explicit ORDER BY duration_sec_percent_rank, id)
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.duration_sec BETWEEN 144 AND 352
              AND ARRAY_CONTAINS(t.moods, 'focus')
        )
      AND s.duration_sec_percent_rank_tercile > 2
)
SELECT
    is_explicit,
    rn,
    duration_sec_percent_rank_tercile,
    GROUPING(is_explicit) AS is_total_is_explicit,
    ROUND(STDDEV(duration_sec_sum_3), 2) AS stddev_duration_sec_sum_3,
    SUM(duration_sec_percent_rank_tercile) AS total_duration_sec_percent_rank_tercile,
    DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days
FROM without_matches
GROUP BY ROLLUP(is_explicit, rn, duration_sec_percent_rank_tercile)
