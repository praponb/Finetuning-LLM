WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE (device = 'phone' OR is_skipped = FALSE)
),
joined_listener AS (
    -- Attach listener attributes
    SELECT s.*, l.country AS listener_country
    FROM filtered s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.country IS NOT NULL
),
combined_details_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.played_at_date,
        s.listener_country,
        t.title AS track_title,
        t.bpm AS track_bpm
    FROM joined_listener s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.bpm <= 147
)
SELECT id, metric, amount
FROM (
    SELECT id, ms_played, track_bpm
    FROM combined_details_track
) src
UNPIVOT (
    amount FOR metric IN (ms_played, track_bpm)
) unp
ORDER BY id, metric
