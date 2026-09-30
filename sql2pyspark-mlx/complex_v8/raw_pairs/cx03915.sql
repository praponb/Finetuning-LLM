WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE duration_sec < 365
      AND release_date IS NOT NULL
),
latest AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id DESC) AS rn
        FROM filtered_tracks
    ) ranked
    WHERE rn = 1
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at
    FROM latest s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        DATE_TRUNC('MONTH', last_played_at) AS last_played_at_month,
        SUM(duration_sec) AS total_duration_sec,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_stats
    GROUP BY artist_id, DATE_TRUNC('MONTH', last_played_at)
    HAVING SUM(duration_sec) > 144
),
tagged AS (
    -- Label and score rows
    SELECT
        artist_id,
        last_played_at_month,
        n_rows,
        CASE WHEN distinct_ids >= 8 THEN 'A' WHEN distinct_ids >= 5 THEN 'B' ELSE 'C' END AS distinct_ids_tier,
        n_rows - distinct_ids AS n_rows_minus_distinct_ids,
        TO_DATE(last_played_at_month) AS last_played_at_month_date
    FROM stats
    WHERE last_played_at_month < TIMESTAMP '2025-02-05 09:00:00'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        artist_id,
        last_played_at_month,
        n_rows,
        n_rows_minus_distinct_ids,
        last_played_at_month_date,
        ROW_NUMBER() OVER (PARTITION BY n_rows, distinct_ids_tier ORDER BY n_rows DESC, artist_id, last_played_at_month) AS rn,
        NTILE(5) OVER (PARTITION BY n_rows, distinct_ids_tier ORDER BY n_rows DESC, artist_id, last_played_at_month) AS n_rows_quintile,
        ROUND(n_rows - AVG(n_rows) OVER w_n_rows, 2) AS n_rows_vs_n_rows_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY n_rows, distinct_ids_tier ORDER BY n_rows DESC), 3) AS n_rows_percent_rank,
        n_rows_minus_distinct_ids - LAG(n_rows_minus_distinct_ids) OVER (ORDER BY n_rows, artist_id, last_played_at_month) AS n_rows_minus_distinct_ids_change,
        COUNT(*) OVER (PARTITION BY n_rows) AS rows_in_n_rows
    FROM tagged
    WINDOW w_n_rows AS (PARTITION BY n_rows)
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
        )
)
-- Final projection
SELECT
    artist_id,
    last_played_at_month,
    n_rows,
    n_rows_minus_distinct_ids,
    last_played_at_month_date,
    rn,
    n_rows_quintile,
    n_rows_percent_rank,
    rows_in_n_rows
FROM without_matches
WHERE n_rows_minus_distinct_ids_change >= -4
ORDER BY rn, last_played_at_month_date NULLS LAST, artist_id, last_played_at_month
LIMIT 5
