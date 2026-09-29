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
        HOUR(played_at) AS played_at_hour,
        played_at + INTERVAL 1 DAYS AS played_at_plus_1d
    FROM plays
    WHERE device IN ('desktop', 'speaker', 'tv')
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM base
    WHERE played_at_hour < (SELECT MAX(played_at_hour) FROM base)
),
with_activity_2 AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_activity s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.source <> 'album'
        )
),
deduped AS (
    -- Keep the latest row per device, track_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        played_at_hour,
        played_at_plus_1d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device, track_id ORDER BY played_at DESC, id DESC) AS row_num
        FROM with_activity_2
    ) ranked
    WHERE row_num = 1
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        played_at + INTERVAL 7 DAYS AS played_at_plus_7d,
        CASE WHEN device IN ('phone', 'car') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group,
        ROUND(ms_played / NULLIF(played_at_hour, 0), 3) AS ms_played_per_played_at_hour
    FROM deduped
    WHERE listener_id IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ms_played,
        device,
        is_skipped,
        played_at_plus_1d,
        played_at_plus_7d,
        ROW_NUMBER() OVER (PARTITION BY listener_id, track_id ORDER BY played_at_hour, id) AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, track_id ORDER BY played_at_hour), 3) AS played_at_hour_cume_dist
    FROM labeled
)
SELECT id, field, value
FROM (
    SELECT id, rn, ms_played
    FROM ranked
) src
UNPIVOT (
    value FOR field IN (rn, ms_played)
) unp
