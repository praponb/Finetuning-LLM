WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag_2,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE ms_played >= 145720
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        track_id,
        ms_played,
        is_skipped_flag,
        played_at_date,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        DATE_TRUNC('YEAR', played_at) AS played_at_year,
        HOUR(played_at) AS played_at_hour
    FROM filtered
),
filtered_2 AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.ms_played < 223828
        )
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        played_at_hour + is_skipped_flag AS played_at_hour_plus_is_skipped_flag
    FROM filtered_2
),
annotated AS (
    -- Add calculated columns
    SELECT *, COALESCE(played_at_hour, 0) AS played_at_hour_filled
    FROM derived
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        ms_played,
        is_skipped_flag,
        played_at_year,
        played_at_hour,
        played_at_hour_plus_is_skipped_flag,
        played_at_hour_filled,
        ROUND(AVG(is_skipped_flag) OVER (PARTITION BY played_at_hour), 2) AS played_at_hour_avg_is_skipped_flag,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY played_at_hour), 2) AS ms_played_vs_played_at_hour_avg
    FROM annotated
)
SELECT
    id,
    track_id,
    is_skipped_flag,
    played_at_year,
    played_at_hour,
    played_at_hour_filled,
    ms_played_vs_played_at_hour_avg,
    DATE_TRUNC('WEEK', played_at_year) AS played_at_year_week
FROM ranked
WHERE played_at_year >= TIMESTAMP '2025-01-17 09:00:00'
ORDER BY played_at_hour NULLS LAST
