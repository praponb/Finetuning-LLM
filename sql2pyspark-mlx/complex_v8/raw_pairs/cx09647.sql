WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN source IN ('radio', 'search') THEN 'group_a' WHEN source = 'album' THEN 'group_b' ELSE 'other' END AS source_group,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM plays
    WHERE (ms_played >= 49807 OR is_skipped = TRUE)
),
with_parent AS (
    -- Attach listener attributes
    SELECT
        s.*,
        l.interests AS listener_interests,
        l.birth_year AS listener_birth_year
    FROM base_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(is_skipped_flag / NULLIF(listener_birth_year, 0), 3) AS is_skipped_flag_per_listener_birth_year
    FROM with_parent
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT listener_id, source, ms_played
    FROM derived
) src
PIVOT (
    SUM(ms_played)
    FOR source IN ('search', 'playlist', 'album')
)
ORDER BY listener_id
