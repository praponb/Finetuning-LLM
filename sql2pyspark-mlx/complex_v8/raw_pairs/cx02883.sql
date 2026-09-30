WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        CONCAT_WS('-', source, device) AS source_device_label
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-12 10:00:00'
      AND device = 'desktop'
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM filtered s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
        )
),
screened_2 AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND l.signup_date BETWEEN DATE '2024-04-16' AND DATE '2025-03-18'
              AND NOT ARRAY_CONTAINS(l.interests, 'indie')
        )
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.played_at AS play_played_at,
        p.device AS play_device,
        p.ms_played AS play_ms_played
    FROM screened_2 s
    JOIN plays p ON p.listener_id = s.listener_id AND p.track_id >= 9
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT play_id, play_device, play_ms_played
    FROM exploded_detail
) src
PIVOT (
    MAX(play_ms_played) AS max_v, SUM(play_ms_played) AS sum_v
    FOR play_device IN ('desktop', 'phone')
)
ORDER BY play_id
