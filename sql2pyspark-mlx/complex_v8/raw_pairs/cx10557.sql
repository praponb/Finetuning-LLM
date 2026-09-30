WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        csat,
        tags,
        CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref
    FROM tickets
    WHERE csat > 2
)
SELECT
    id,
    agent_id,
    channel,
    csat,
    tags
FROM cleaned_tickets
