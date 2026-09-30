WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        LOWER(TRIM(source)) AS source_clean
    FROM plays
    WHERE ms_played BETWEEN 92277 AND 351217
      AND track_id IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ms_played - LAG(ms_played) OVER w_listener_id_by_ms_played AS ms_played_change,
        LAG(ms_played, 1, 0) OVER (PARTITION BY source ORDER BY played_at ASC, id) AS prev_ms_played,
        LEAD(ms_played, 2) OVER w_all_by_played_at AS next2_ms_played,
        NTILE(5) OVER (ORDER BY played_at NULLS LAST, id) AS played_at_quintile,
        LAG(played_at) OVER (PARTITION BY listener_id ORDER BY ms_played ASC, id) AS prev_played_at,
        SIZE(COLLECT_SET(listener_id) OVER w_source) AS distinct_listener_id_in_source,
        NTILE(3) OVER (PARTITION BY listener_id ORDER BY ms_played ASC, id) AS ms_played_tercile
    FROM base
    WINDOW w_listener_id_by_ms_played AS (PARTITION BY listener_id ORDER BY ms_played ASC, id), w_all_by_played_at AS (ORDER BY played_at NULLS LAST, id), w_source AS (PARTITION BY source)
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        source,
        source_clean,
        ms_played_change,
        prev_ms_played,
        next2_ms_played,
        played_at_quintile,
        prev_played_at,
        distinct_listener_id_in_source,
        ms_played_tercile,
        LEAST(ms_played_change, ms_played) AS least_ms_played_change_ms_played
    FROM ranked
    WHERE next2_ms_played <= 299017
)
SELECT
    id,
    listener_id,
    played_at,
    source,
    prev_ms_played,
    next2_ms_played,
    played_at_quintile,
    prev_played_at,
    distinct_listener_id_in_source,
    ms_played_tercile,
    least_ms_played_change_ms_played
FROM labeled
ORDER BY prev_ms_played DESC, prev_played_at NULLS LAST
