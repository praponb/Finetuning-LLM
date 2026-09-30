WITH scoped AS (
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
        CASE WHEN device IN ('speaker', 'phone') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group,
        CASE WHEN device IN ('desktop', 'tv') THEN 'group_a' WHEN device = 'speaker' THEN 'group_b' ELSE 'other' END AS device_group_2
    FROM plays
    WHERE MONTH(played_at) IN (2, 3)
),
totals AS (
    -- Group-level totals
    SELECT
        listener_id,
        track_id,
        device,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        SUM(ms_played) AS total_ms_played,
        MIN(played_at) AS first_played_at
    FROM scoped
    GROUP BY listener_id, track_id, device
    HAVING SUM(ms_played) > 99376
),
with_children AS (
    -- One row per play
    SELECT
        s.listener_id,
        s.track_id,
        s.device,
        s.total_ms_played,
        s.first_played_at,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.listener_id AS play_listener_id
    FROM totals s
    JOIN plays p ON p.track_id = s.track_id AND p.ms_played < 225733
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_children s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.play_listener_id
              AND p.device NOT IN ('tv', 'speaker')
              AND p.ms_played < 333795
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT listener_id, play_id, device, total_ms_played
    FROM screened
) src
PIVOT (
    SUM(total_ms_played)
    FOR device IN ('desktop', 'phone', 'tv')
)
ORDER BY listener_id, play_id
