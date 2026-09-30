WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-24 10:00:00'
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM filtered_plays
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        *,
        TO_DATE(played_at) AS played_at_date,
        DAYOFWEEK(played_at) AS played_at_dayofweek,
        CASE WHEN ms_played >= 301149 THEN 'HIGH' WHEN ms_played >= 112786 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier
    FROM derived
    WHERE listener_id <= 4
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    is_skipped,
    is_skipped_label,
    played_at_date,
    ms_played_tier
FROM derived_2
ORDER BY ms_played_tier DESC
