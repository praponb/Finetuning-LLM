WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-18 11:00:00'
),
combined_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.actor_slug,
        s.event_at_date,
        t.csat AS ticket_csat,
        t.channel AS ticket_channel,
        t.tags AS ticket_tags
    FROM scoped_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.closed_at >= TIMESTAMP '2025-01-14 23:00:00'
),
flattened AS (
    -- One row per ticket_tag
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.actor_slug,
        s.event_at_date,
        s.ticket_csat,
        s.ticket_channel,
        t.ticket_tag
    FROM combined_details s
    LATERAL VIEW EXPLODE(s.ticket_tags) t AS ticket_tag
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(ticket_csat) OVER w_actor_ticket_channel AS actor_ticket_channel_sum_ticket_csat,
        FIRST_VALUE(event_at_date) OVER (PARTITION BY ticket_channel, ticket_tag ORDER BY ticket_csat ASC, id, ticket_tag ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_date
    FROM flattened
    WINDOW w_actor_ticket_channel AS (PARTITION BY actor, ticket_channel)
),
with_parent_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.actor_slug,
        s.event_at_date,
        s.ticket_csat,
        s.ticket_channel,
        s.ticket_tag,
        s.actor_ticket_channel_sum_ticket_csat,
        s.first_event_at_date,
        t.priority AS ticket_priority,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags,
        su.agent_name
    FROM with_windows s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
    WHERE t.priority = 'P3'
)
-- Subtotals with GROUPING SETS
SELECT
    ticket_csat,
    ticket_tag,
    ROUND(AVG(ticket_csat), 2) AS avg_ticket_csat,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_parent_ticket
GROUP BY GROUPING SETS ((ticket_csat, ticket_tag), (ticket_csat), (ticket_tag), ())
ORDER BY ticket_csat NULLS LAST, ticket_tag NULLS LAST
