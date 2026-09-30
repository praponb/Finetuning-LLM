WITH cleaned_plays AS (
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
        CONCAT(device, '#', CAST(track_id AS STRING)) AS device_ref
    FROM plays
    WHERE source IN ('album', 'search', 'radio')
      AND ms_played > 67932
),
all_events AS (
    -- Stack the branches
    SELECT ms_played, played_at, device AS category
    FROM cleaned_plays
    WHERE track_id >= 21
      AND HOUR(played_at) BETWEEN 6 AND 10
    UNION
    SELECT ms_played, played_at, source
    FROM plays
)
SELECT category, COUNT(*) AS n_rows, SUM(ms_played) AS total_ms_played, MAX(played_at) AS latest_played_at
FROM all_events
GROUP BY category
ORDER BY category
