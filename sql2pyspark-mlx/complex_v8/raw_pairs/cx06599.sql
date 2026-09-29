WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(actor)) AS actor_clean,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE actor = 'agent'
      AND event_type IN ('created', 'replied')
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_clean,
        s.actor_group,
        t.opened_at AS ticket_opened_at,
        t.channel AS ticket_channel,
        su.tier AS agent_tier
    FROM cleaned_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_details s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND TO_DATE(t.event_at) < DATE '2025-02-12'
        )
      AND s.actor_clean IS NOT NULL
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.actor_group,
        s.ticket_opened_at,
        s.ticket_channel,
        s.agent_tier,
        t.opened_at AS ticket_opened_at_2,
        t.priority AS ticket_priority,
        su.team AS agent_team,
        su.tier AS agent_tier_2
    FROM qualified s
    JOIN tickets t ON t.id = s.ticket_id AND ARRAY_CONTAINS(t.tags, 'refund')
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
enriched_2 AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.actor_group,
        s.ticket_opened_at,
        s.ticket_channel,
        s.agent_tier,
        s.ticket_opened_at_2,
        s.ticket_priority,
        s.agent_team,
        t.agent_id AS ticket_agent_id
    FROM enriched s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        actor,
        actor_group,
        agent_tier,
        ticket_opened_at_2,
        ticket_priority,
        agent_team,
        ticket_agent_id,
        ROUND(CUME_DIST() OVER w_ticket_channel_by_ticket_ope, 3) AS ticket_opened_at_2_cume_dist,
        LAG(ticket_opened_at_2, 2) OVER (PARTITION BY ticket_channel ORDER BY ticket_opened_at_2 DESC, id) AS prev2_ticket_opened_at_2
    FROM enriched_2
    WINDOW w_ticket_channel_by_ticket_ope AS (PARTITION BY ticket_channel ORDER BY ticket_opened_at_2 DESC)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        actor,
        actor_group,
        agent_tier,
        ticket_opened_at_2,
        ticket_priority,
        agent_team,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor_group, ticket_agent_id ORDER BY ticket_opened_at_2_cume_dist DESC), 3) AS ticket_opened_at_2_cume_dist_percent_rank,
        SUM(ticket_opened_at_2_cume_dist) OVER (PARTITION BY actor_group, ticket_agent_id ORDER BY ticket_opened_at_2_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ticket_opened_at_2_cume_dist,
        SIZE(COLLECT_SET(ticket_agent_id) OVER (PARTITION BY agent_team)) AS distinct_ticket_agent_id_in_agent_team
    FROM with_windows
)
SELECT
    id,
    actor_group,
    agent_tier,
    ticket_opened_at_2,
    ticket_priority,
    ticket_opened_at_2_cume_dist_percent_rank,
    distinct_ticket_agent_id_in_agent_team
FROM sequenced
WHERE ticket_opened_at_2_cume_dist_percent_rank BETWEEN 0.1 AND 0.7
ORDER BY agent_tier NULLS LAST, id
LIMIT 20
