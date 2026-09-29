WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        played_at + INTERVAL 15 MINUTES AS played_at_plus_15m
    FROM plays
    WHERE is_skipped = TRUE
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        l.interests AS listener_interests,
        l.plan AS listener_plan
    FROM valid_plays s
    JOIN listeners l ON l.id = s.listener_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        MONTH(played_at) AS played_at_month,
        CONCAT(listener_plan, '#', CAST(track_id AS STRING)) AS listener_plan_ref,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM with_parent
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN listener_plan IN ('premium', 'student') THEN 'group_a' WHEN listener_plan = 'family' THEN 'group_b' ELSE 'other' END AS listener_plan_group
    FROM annotated
    WHERE is_skipped = TRUE
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    source,
    listener_interests,
    played_at_month,
    listener_plan_ref,
    is_skipped_flag,
    listener_plan_group,
    ABS(played_at_month - 6) AS played_at_month_distance,
    UPPER(TRIM(listener_plan_ref)) AS listener_plan_ref_clean
FROM labeled
ORDER BY listener_id, listener_plan_ref_clean
