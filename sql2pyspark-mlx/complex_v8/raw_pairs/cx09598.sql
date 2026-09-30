WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        t.bpm AS track_bpm,
        t.moods AS track_moods,
        t.duration_sec AS track_duration_sec,
        a.debut_year AS artist_debut_year
    FROM scoped s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
    WHERE t.duration_sec >= 142
)
SELECT
    track_id,
    is_skipped,
    COALESCE(device, 'ALL') AS device,
    GROUPING_ID() AS gid,
    COUNT(*) AS n_rows,
    COUNT(*) FILTER (WHERE NOT is_skipped) AS n_not_is_skipped
FROM with_details
GROUP BY ROLLUP(track_id, is_skipped, device)
