WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'bug')
),
combined AS (
    -- Stack the branches
    SELECT requester_id AS qty, opened_at, csat
    FROM prepared
    UNION
    SELECT ticket_id, event_at, id
    FROM ticket_events
    WHERE event_type = 'assigned'
)
SELECT qty, COUNT(*) AS n_rows, SUM(csat) AS total_csat, MAX(opened_at) AS latest_opened_at
FROM combined
GROUP BY qty
ORDER BY qty
