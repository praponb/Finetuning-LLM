WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 10:00:00'
),
totals AS (
    -- Group-level totals
    SELECT
        actor,
        ticket_id,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
        ROUND(100 * COUNT(CASE WHEN actor = 'system' THEN 1 END) / COUNT(*), 1) AS pct_actor_system
    FROM filtered
    GROUP BY actor, ticket_id
),
joined AS (
    -- Join tickets
    SELECT
        s.actor,
        s.ticket_id,
        s.distinct_ticket_ids,
        t.opened_at AS ticket_opened_at,
        t.csat AS ticket_csat,
        t.agent_id AS ticket_agent_id
    FROM totals s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.actor,
        s.ticket_id,
        s.distinct_ticket_ids,
        s.ticket_opened_at,
        s.ticket_csat,
        s.ticket_agent_id,
        t.opened_at AS ticket_opened_at_2,
        t.channel AS ticket_channel,
        su.agent_name
    FROM joined s
    JOIN tickets t ON t.id = s.ticket_id AND t.priority NOT IN ('P4', 'P3')
    JOIN support_agents su ON su.id = t.agent_id
),
enriched_agent AS (
    -- Join support_agents
    SELECT
        s.actor,
        s.ticket_id,
        s.distinct_ticket_ids,
        s.ticket_opened_at,
        s.ticket_agent_id,
        s.ticket_opened_at_2,
        s.ticket_channel,
        s.agent_name,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.agent_name AS agent_agent_name,
        t.csat AS ticket_csat_2,
        t.opened_at AS ticket_opened_at_3
    FROM with_parent_ticket s
    JOIN support_agents su ON su.id = s.ticket_agent_id
    JOIN tickets t ON t.id = s.ticket_id
)
-- Subtotals with ROLLUP
SELECT
    ticket_id,
    COALESCE(ticket_channel, 'ALL') AS ticket_channel,
    ticket_agent_id,
    COUNT(DISTINCT ticket_id) AS distinct_ticket_ids_2
FROM enriched_agent
GROUP BY ROLLUP(ticket_id, ticket_channel, ticket_agent_id)
