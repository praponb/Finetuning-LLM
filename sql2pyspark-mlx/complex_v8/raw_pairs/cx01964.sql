WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label,
        CASE WHEN ms_played >= 287769 THEN 'HIGH' WHEN ms_played >= 108979 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier
    FROM plays
    WHERE (source = 'album' OR is_skipped = TRUE)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        played_at_label,
        ms_played_tier,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix,
        ABS(ms_played - 112786) AS ms_played_distance,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM scoped_plays
)
SELECT id, measure, amount
FROM (
    SELECT id, ms_played_distance, ms_played
    FROM tagged
) src
UNPIVOT (
    amount FOR measure IN (ms_played_distance, ms_played)
) unp
