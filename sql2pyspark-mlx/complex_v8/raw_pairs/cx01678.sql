WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        HOUR(played_at) AS played_at_hour,
        LENGTH(TRIM(device)) AS device_len,
        MONTH(played_at) AS played_at_month
    FROM plays
    WHERE (is_skipped = TRUE OR device <> 'desktop')
),
with_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.played_at_hour,
        s.played_at_month,
        l.username AS listener_username,
        t.duration_sec AS track_duration_sec
    FROM valid_plays s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_details_listener s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.played_at < TIMESTAMP '2025-03-08 10:00:00'
              AND p.listener_id IS NOT NULL
        )
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.device = 'phone'
        )
),
enriched_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.played_at_hour,
        s.played_at_month,
        s.listener_username,
        s.track_duration_sec,
        l.interests AS listener_interests,
        t.moods AS track_moods,
        t.title AS track_title
    FROM screened s
    JOIN listeners l ON l.id = s.listener_id AND l.plan <> 'student'
    LEFT JOIN tracks t ON t.id = s.track_id
    WHERE SIZE(l.interests) >= 2
),
with_details AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.played_at_hour,
        s.played_at_month,
        s.listener_username,
        s.track_title,
        l.plan AS listener_plan
    FROM enriched_listener s
    JOIN listeners l ON l.id = s.listener_id
)
SELECT *
FROM (
    SELECT played_at_month, device, listener_plan, played_at_hour
    FROM with_details
) src
PIVOT (
    MAX(played_at_hour)
    FOR listener_plan IN ('premium', 'student')
)
ORDER BY played_at_month, device
