WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
),
with_parent_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        l.plan AS listener_plan,
        l.username AS listener_username,
        t.moods AS track_moods,
        t.title AS track_title
    FROM valid_plays s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
    WHERE l.plan IS NOT NULL
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    is_skipped,
    listener_plan,
    listener_username,
    track_moods,
    track_title
FROM with_parent_listener
WHERE listener_plan IS NOT NULL
ORDER BY listener_username DESC NULLS FIRST, track_title DESC NULLS FIRST, id
LIMIT 10
