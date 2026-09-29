WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-02-18'
      AND ms_played > 120663
),
latest AS (
    -- Keep the latest row per listener_id, device
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        ms_played_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id, device ORDER BY ms_played_filled DESC, id DESC) AS row_num
        FROM scoped
    ) ranked
    WHERE row_num = 1
),
enriched_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        s.ms_played_filled,
        t.is_explicit AS track_is_explicit,
        l.username AS listener_username
    FROM latest s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
    WHERE t.is_explicit = TRUE
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    is_skipped,
    source,
    ms_played_filled,
    track_is_explicit,
    listener_username,
    FLOOR(ms_played_filled / 50000) * 50000 AS ms_played_filled_bucket
FROM enriched_track
ORDER BY listener_id DESC, source
