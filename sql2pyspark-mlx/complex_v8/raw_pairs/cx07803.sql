WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label,
        DAYOFWEEK(played_at) AS played_at_dayofweek
    FROM plays
    WHERE device IS NOT NULL
      AND source = 'radio'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        played_at_label,
        played_at_dayofweek,
        MAX(ms_played) OVER (PARTITION BY is_skipped) AS is_skipped_max_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY device ORDER BY played_at_dayofweek DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_4,
        LAG(played_at, 3) OVER w_track_id_played_at_to_date_b AS prev3_played_at,
        LAST_VALUE(played_at_dayofweek) IGNORE NULLS OVER (PARTITION BY track_id, TO_DATE(played_at) ORDER BY played_at_dayofweek DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_dayofweek,
        ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY played_at_dayofweek, id) AS rn,
        LAST_VALUE(ms_played) OVER (PARTITION BY track_id ORDER BY played_at_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        LAST_VALUE(played_at) OVER (PARTITION BY track_id ORDER BY played_at_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        ROUND(AVG(played_at_dayofweek) OVER (PARTITION BY track_id, TO_DATE(played_at) ORDER BY played_at_dayofweek DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS played_at_dayofweek_avg_6_prior
    FROM filtered_plays
    WINDOW w_track_id_played_at_to_date_b AS (PARTITION BY track_id, TO_DATE(played_at) ORDER BY played_at_dayofweek DESC NULLS FIRST, id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(played_at_dayofweek, rn) AS greatest_played_at_dayofweek_rn,
        ABS(played_at_dayofweek - 3) AS played_at_dayofweek_distance
    FROM sequenced
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        greatest_played_at_dayofweek_rn,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT track_id) AS distinct_track_ids,
        MAX(last_played_at) AS last_last_played_at,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids
    FROM derived
    GROUP BY greatest_played_at_dayofweek_rn
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(distinct_listener_ids, 3) OVER w_n_rows_by_distinct_ids AS next3_distinct_listener_ids,
        NTILE(3) OVER w_n_rows_by_distinct_ids AS distinct_ids_tercile
    FROM grouped
    WINDOW w_n_rows_by_distinct_ids AS (PARTITION BY n_rows ORDER BY distinct_ids DESC, greatest_played_at_dayofweek_rn)
)
-- Final projection
SELECT
    greatest_played_at_dayofweek_rn,
    n_rows,
    distinct_track_ids,
    last_last_played_at,
    distinct_ids,
    distinct_listener_ids,
    next3_distinct_listener_ids,
    distinct_ids_tercile,
    ROUND(distinct_ids_tercile / NULLIF(n_rows, 0), 3) AS distinct_ids_tercile_per_n_rows
FROM windowed
WHERE last_last_played_at >= TIMESTAMP '2025-01-31 12:00:00'
ORDER BY greatest_played_at_dayofweek_rn DESC NULLS FIRST, n_rows
