WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
    FROM plays
)
SELECT played_at_label, played_at AS event_time
FROM prepared_plays
UNION ALL
SELECT device AS played_at_label, played_at AS event_time
FROM plays
WHERE ms_played >= 49807
  AND MONTH(played_at) IN (1, 3)
