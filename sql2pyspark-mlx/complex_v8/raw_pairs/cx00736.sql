WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source NOT IN ('radio', 'album')
      AND NOT is_skipped
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.listener_id = s.listener_id) AS max_play_ms_played,
        (SELECT COUNT(CASE WHEN p.is_skipped = FALSE THEN 1 END) FROM plays p WHERE p.listener_id = s.listener_id) AS play_is_skipped_count,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at
    FROM base s
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(ms_played, max_play_ms_played) AS least_ms_played_max_play_ms_played
    FROM summarized_rows
    WHERE is_skipped = FALSE
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS last_played_at
    FROM labeled s
),
labeled_2 AS (
    -- Derive helper columns
    SELECT *, DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
    FROM with_stats
    WHERE last_played_at < TIMESTAMP '2025-02-05 09:00:00'
),
summary AS (
    -- Summarise per group
    SELECT
        TO_DATE(first_played_at) AS first_played_at_day,
        source,
        device,
        PERCENTILE_APPROX(least_ms_played_max_play_ms_played, 0.5) AS median_least_ms_played_max_play_ms_played,
        MAX(last_played_at) AS last_last_played_at,
        MIN(least_ms_played_max_play_ms_played) AS min_least_ms_played_max_play_ms_played,
        MIN(first_played_at) AS first_first_played_at
    FROM labeled_2
    GROUP BY 1, 2, 3
)
-- Native SQL UNPIVOT (wide to long)
SELECT first_played_at_day, source, metric_name, amount
FROM (
    SELECT first_played_at_day, source, min_least_ms_played_max_play_ms_played, median_least_ms_played_max_play_ms_played
    FROM summary
) src
UNPIVOT (
    amount FOR metric_name IN (min_least_ms_played_max_play_ms_played, median_least_ms_played_max_play_ms_played)
) unp
