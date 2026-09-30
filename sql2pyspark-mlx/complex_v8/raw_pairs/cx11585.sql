WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE track_id >= 11
      AND listener_id IS NOT NULL
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        is_skipped_flag,
        MIN(ms_played) AS min_ms_played,
        COUNT(*) FILTER (WHERE NOT is_skipped) AS n_not_is_skipped
    FROM valid
    GROUP BY DATE_TRUNC('MONTH', played_at), is_skipped_flag
    HAVING MAX(is_skipped_flag) > 0
),
enriched AS (
    -- Add calculated columns
    SELECT
        played_at_month,
        is_skipped_flag,
        min_ms_played,
        n_not_is_skipped,
        GREATEST(n_not_is_skipped, is_skipped_flag) AS greatest_n_not_is_skipped_is_skipped_flag,
        TO_DATE(played_at_month) AS played_at_month_date,
        ABS(n_not_is_skipped - 3) AS n_not_is_skipped_distance
    FROM summary
    WHERE n_not_is_skipped >= 2
),
labeled AS (
    -- Add calculated columns
    SELECT
        played_at_month,
        is_skipped_flag,
        min_ms_played,
        greatest_n_not_is_skipped_is_skipped_flag,
        played_at_month_date,
        n_not_is_skipped_distance,
        TRUNC(played_at_month_date, 'MM') AS played_at_month_date_month_start
    FROM enriched
    WHERE greatest_n_not_is_skipped_is_skipped_flag > 2
)
-- Final projection
SELECT
    played_at_month,
    is_skipped_flag,
    min_ms_played,
    greatest_n_not_is_skipped_is_skipped_flag,
    n_not_is_skipped_distance,
    played_at_month_date_month_start,
    HOUR(played_at_month) AS played_at_month_hour
FROM labeled
