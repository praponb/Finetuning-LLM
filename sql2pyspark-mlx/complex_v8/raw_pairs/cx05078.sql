WITH base AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        INITCAP(TRIM(channel)) AS channel_clean
    FROM tickets
    WHERE MONTH(closed_at) IN (1, 2)
),
combined AS (
    -- Stack the branches
    SELECT 'ticket' AS src, channel_clean, opened_at AS happened_at
    FROM base
    UNION ALL
    SELECT 'event' AS src, event_type AS channel_clean, event_at AS happened_at
    FROM ticket_events
    WHERE ticket_id <= 52
    UNION ALL
    SELECT 'event' AS src, actor AS channel_clean, event_at AS happened_at
    FROM ticket_events
)
SELECT src, COUNT(*) AS n_rows, MAX(happened_at) AS latest_happened_at
FROM combined
GROUP BY src
ORDER BY src
