WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type NOT IN ('assigned', 'resolved')
      AND actor <> 'customer'
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        t.channel AS ticket_channel,
        t.opened_at AS ticket_opened_at,
        t.requester_id AS ticket_requester_id,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        actor,
        ticket_channel,
        ticket_opened_at,
        agent_team,
        agent_tier,
        SUM(ticket_requester_id) OVER (PARTITION BY actor, ticket_channel) AS actor_ticket_channel_sum_ticket_requester_id,
        SIZE(COLLECT_SET(agent_tier) OVER w_actor_ticket_channel) AS distinct_agent_tier_in_actor_ticket_channel,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY ticket_channel ORDER BY ticket_requester_id ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ticket_requester_id_avg_3,
        NTH_VALUE(ticket_requester_id, 2) OVER (PARTITION BY agent_tier, agent_team ORDER BY ticket_opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_requester_id
    FROM with_details_ticket
    WINDOW w_actor_ticket_channel AS (PARTITION BY actor, ticket_channel)
),
stats AS (
    -- Summarise per group
    SELECT
        actor,
        agent_team,
        YEAR(ticket_opened_at) AS ticket_opened_at_year,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list,
        COUNT(DISTINCT agent_tier) AS distinct_agent_tiers,
        MIN(second_ticket_requester_id) AS min_second_ticket_requester_id,
        COUNT(DISTINCT ticket_channel) AS distinct_ticket_channels,
        SUM(CASE WHEN agent_team IN ('Onboarding', 'Tech', 'Billing') THEN 1 ELSE 0 END) AS n_agent_team_in_onboarding_tec
    FROM windowed
    WHERE actor <> 'customer'
    GROUP BY actor, agent_team, YEAR(ticket_opened_at)
)
-- Final projection
SELECT
    actor,
    agent_team,
    ticket_opened_at_year,
    row_count,
    agent_team_list,
    distinct_agent_tiers,
    min_second_ticket_requester_id,
    distinct_ticket_channels,
    n_agent_team_in_onboarding_tec
FROM stats
ORDER BY agent_team DESC NULLS FIRST, actor DESC, ticket_opened_at_year
LIMIT 20
