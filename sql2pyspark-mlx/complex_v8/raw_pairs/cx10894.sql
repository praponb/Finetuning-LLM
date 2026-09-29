WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        LOWER(TRIM(device)) AS device_clean
    FROM plays
),
with_parent_listener AS (
    -- Join listeners
    SELECT
        s.*,
        l.plan AS listener_plan,
        l.interests AS listener_interests,
        l.birth_year AS listener_birth_year
    FROM recent_plays s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.birth_year < 1994
)
-- Subtotals with CUBE
SELECT
    COALESCE(source, 'ALL') AS source,
    listener_id,
    ARRAY_SORT(COLLECT_SET(listener_plan)) AS listener_plan_list
FROM with_parent_listener
GROUP BY CUBE(source, listener_id)
ORDER BY source NULLS LAST, listener_id NULLS LAST
