WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        UPPER(TRIM(title)) AS title_clean,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE SIZE(moods) >= 1
      AND duration_sec < 352
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at_2
    FROM cleaned s
),
summary AS (
    -- Group-level totals
    SELECT
        is_explicit,
        title_len,
        ROUND(AVG(bpm), 2) AS avg_bpm,
        COUNT(*) AS cnt,
        SUM(CASE WHEN title IS NOT NULL THEN 1 ELSE 0 END) AS n_title_is_not_null,
        MIN(duration_sec) AS min_duration_sec
    FROM with_stats
    GROUP BY 1, 2
    HAVING COUNT(*) >= 3
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        is_explicit,
        title_len,
        avg_bpm,
        n_title_is_not_null,
        min_duration_sec,
        NTILE(3) OVER (ORDER BY n_title_is_not_null, is_explicit, title_len) AS n_title_is_not_null_tercile,
        LAST_VALUE(avg_bpm) OVER (ORDER BY n_title_is_not_null, is_explicit, title_len ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_avg_bpm,
        FIRST_VALUE(title_len) IGNORE NULLS OVER (ORDER BY n_title_is_not_null, is_explicit, title_len ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_title_len,
        SUM(n_title_is_not_null) OVER (ORDER BY n_title_is_not_null, is_explicit, title_len ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS n_title_is_not_null_sum_2,
        COUNT(*) OVER (PARTITION BY is_explicit, title_len) AS rows_in_is_explicit_title_len
    FROM summary
)
SELECT is_explicit, title_len, field, value
FROM (
    SELECT is_explicit, title_len, avg_bpm, last_avg_bpm
    FROM ranked
) src
UNPIVOT (
    value FOR field IN (avg_bpm, last_avg_bpm)
) unp
