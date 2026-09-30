WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix
    FROM plays
    WHERE listener_id >= 6
),
with_parent AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.device_prefix,
        l.interests AS listener_interests,
        l.username AS listener_username,
        t.is_explicit AS track_is_explicit
    FROM base_plays s
    JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
with_parent_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.device_prefix,
        s.listener_interests,
        s.listener_username,
        s.track_is_explicit,
        l.plan AS listener_plan,
        l.birth_year AS listener_birth_year,
        l.username AS listener_username_2
    FROM with_parent s
    LEFT JOIN listeners l ON l.id = s.listener_id AND l.plan IS NOT NULL
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_parent_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND NOT p.is_skipped
        )
      AND s.listener_plan = 'free'
)
SELECT id, field, amount
FROM (
    SELECT id, listener_birth_year, ms_played
    FROM with_activity
) src
UNPIVOT (
    amount FOR field IN (listener_birth_year, ms_played)
) unp
