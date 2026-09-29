WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(event_type)) AS event_type_clean,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-24 11:00:00'
),
joined_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_ref,
        t.priority AS ticket_priority,
        t.csat AS ticket_csat,
        t.requester_id AS ticket_requester_id,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(event_at) OVER (PARTITION BY agent_team ORDER BY event_at DESC, id) AS next_event_at,
        ROUND(PERCENT_RANK() OVER w_all_by_ticket_csat, 3) AS ticket_csat_percent_rank,
        COUNT(*) OVER (PARTITION BY actor, event_type) AS rows_in_actor_event_type,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY event_type, ticket_priority), 2) AS ticket_requester_id_vs_event_type_ticket_priorit,
        LAG(ticket_requester_id, 3, 0) OVER (PARTITION BY agent_team ORDER BY event_at DESC, id) AS prev3_ticket_requester_id,
        ROUND(CUME_DIST() OVER w_agent_team_by_event_at_ties, 3) AS event_at_cume_dist,
        DENSE_RANK() OVER (ORDER BY ticket_requester_id DESC) AS ticket_requester_id_dense_rank,
        MAX(ticket_requester_id) OVER w_actor_event_type AS actor_event_type_max_ticket_requester_id,
        ROW_NUMBER() OVER w_agent_team_by_event_at AS seq_no,
        ROUND(ticket_csat - AVG(ticket_csat) OVER w_event_type_ticket_priority, 2) AS ticket_csat_vs_event_type_ticket_priority_avg,
        NTH_VALUE(ticket_csat, 2) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_csat,
        MAX(ticket_requester_id) OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ticket_requester_id_max_3,
        SIZE(COLLECT_SET(agent_tier) OVER (PARTITION BY actor, event_type)) AS distinct_agent_tier_in_actor_event_type,
        COUNT(*) OVER (PARTITION BY event_type, ticket_priority) AS rows_in_event_type_ticket_priority,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY event_type, ticket_priority)) AS distinct_agent_team_in_event_type_ticket_priorit
    FROM joined_ticket
    WINDOW w_all_by_ticket_csat AS (ORDER BY ticket_csat DESC, id), w_agent_team_by_event_at_ties AS (PARTITION BY agent_team ORDER BY event_at DESC), w_actor_event_type AS (PARTITION BY actor, event_type), w_agent_team_by_event_at AS (PARTITION BY agent_team ORDER BY event_at DESC, id), w_event_type_ticket_priority AS (PARTITION BY event_type, ticket_priority)
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_ref,
        ticket_priority,
        ticket_csat,
        ticket_requester_id,
        agent_tier,
        agent_team,
        next_event_at,
        ticket_csat_percent_rank,
        rows_in_actor_event_type,
        ticket_requester_id_vs_event_type_ticket_priorit,
        prev3_ticket_requester_id,
        event_at_cume_dist,
        ticket_requester_id_dense_rank,
        actor_event_type_max_ticket_requester_id,
        ticket_csat_vs_event_type_ticket_priority_avg,
        second_ticket_csat,
        ticket_requester_id_max_3,
        distinct_agent_tier_in_actor_event_type,
        rows_in_event_type_ticket_priority,
        distinct_agent_team_in_event_type_ticket_priorit
    FROM with_trends
    WHERE seq_no <= 2
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    ticket_priority,
    ticket_csat,
    ticket_requester_id,
    next_event_at,
    ticket_csat_percent_rank,
    rows_in_actor_event_type,
    ticket_requester_id_vs_event_type_ticket_priorit,
    event_at_cume_dist,
    ticket_requester_id_dense_rank,
    ticket_csat_vs_event_type_ticket_priority_avg,
    second_ticket_csat,
    ticket_requester_id_max_3,
    distinct_agent_tier_in_actor_event_type,
    rows_in_event_type_ticket_priority,
    distinct_agent_team_in_event_type_ticket_priorit,
    LEAST(event_at_cume_dist, ticket_csat_percent_rank) AS least_event_at_cume_dist_ticket_csat_percent_ran,
    DATE_TRUNC('HOUR', event_at) AS event_at_hour
FROM leaders
WHERE actor <> 'system'
ORDER BY ticket_priority, distinct_agent_team_in_event_type_ticket_priorit DESC
