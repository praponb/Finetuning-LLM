WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
    FROM plays
    WHERE track_id >= 16
),
stacked AS (
    -- Stack the branches
    SELECT 'play' AS source, ms_played AS ref_id, played_at AS event_time
    FROM scoped
    WHERE is_skipped = FALSE
      AND played_at_label IS NOT NULL
    UNION
    SELECT 'play_2' AS source, listener_id AS ref_id, played_at AS event_time
    FROM plays
    WHERE NOT is_skipped
)
SELECT source, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id, MAX(event_time) AS latest_event_time
FROM stacked
GROUP BY source
ORDER BY source
