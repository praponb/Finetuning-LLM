WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        csat,
        tags,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE requester_id >= 7
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    csat,
    tags
FROM prepared_tickets
