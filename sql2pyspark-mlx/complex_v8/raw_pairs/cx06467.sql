WITH valid_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        hired_on
    FROM support_agents
    WHERE team = 'Onboarding'
),
exploded_detail_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.hired_on,
        t.id AS ticket_id,
        t.channel AS ticket_channel,
        t.closed_at AS ticket_closed_at
    FROM valid_agents s
    JOIN tickets t ON t.agent_id = s.id
),
with_children_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.ticket_channel,
        t.id AS ticket_id_2,
        t.csat AS ticket_csat,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel_2
    FROM exploded_detail_tickets s
    LEFT JOIN tickets t ON t.agent_id = s.id
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(ticket_channel)) AS ticket_channel_len,
        LOWER(REGEXP_REPLACE(TRIM(ticket_channel), ' ', '_')) AS ticket_channel_slug
    FROM with_children_tickets
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.id) AS first_ticket_closed_at,
        (SELECT ROUND(AVG(t.requester_id), 2) FROM tickets t WHERE t.agent_id = s.id) AS avg_ticket_requester_id,
        (SELECT MAX(t.requester_id) FROM tickets t WHERE t.agent_id = s.id) AS max_ticket_requester_id
    FROM labeled s
),
enriched AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.ticket_id_2,
        s.ticket_csat,
        s.ticket_tags,
        s.ticket_channel_2,
        s.ticket_channel_len,
        s.ticket_channel_slug,
        s.avg_ticket_requester_id,
        s.max_ticket_requester_id,
        t.opened_at AS ticket_opened_at,
        t.agent_id AS ticket_agent_id,
        t.requester_id AS ticket_requester_id,
        su.agent_name
    FROM summarized_rows s
    JOIN tickets t ON t.id = s.ticket_id_2
    JOIN support_agents su ON su.id = t.agent_id
)
SELECT
    id,
    ticket_csat,
    ticket_tags,
    ticket_channel_len,
    ticket_channel_slug,
    avg_ticket_requester_id,
    ticket_opened_at,
    ticket_agent_id,
    ticket_requester_id,
    agent_name
FROM enriched
ORDER BY avg_ticket_requester_id NULLS LAST, ticket_agent_id
