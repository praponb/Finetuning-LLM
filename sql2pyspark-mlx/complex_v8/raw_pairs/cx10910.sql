WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
rollup_base AS (
    -- Summarise per group
    SELECT
        actor,
        event_type,
        ticket_id,
        SUM(CASE WHEN actor = 'customer' THEN 1 ELSE 0 END) AS n_actor_customer,
        COUNT(DISTINCT actor) AS distinct_actors,
        COUNT(*) FILTER (WHERE event_type IN ('resolved', 'assigned')) AS n_event_type_in_resolved_assig,
        COUNT(*) AS n_rows
    FROM filtered
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
ranked AS (
    -- Previous / next values per key
    SELECT
        actor,
        event_type,
        ticket_id,
        n_actor_customer,
        distinct_actors,
        n_event_type_in_resolved_assig,
        n_rows,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY actor)) AS distinct_ticket_id_in_actor,
        COUNT(*) OVER w_actor AS rows_in_actor,
        NTILE(10) OVER (PARTITION BY event_type, n_actor_customer ORDER BY n_actor_customer DESC, actor, event_type, ticket_id) AS n_actor_customer_decile,
        ROUND(AVG(n_rows) OVER (PARTITION BY event_type, n_actor_customer ORDER BY n_actor_customer DESC, actor, event_type, ticket_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS n_rows_avg_7,
        ROUND(AVG(distinct_actors) OVER (PARTITION BY event_type, n_actor_customer ORDER BY n_actor_customer DESC, actor, event_type, ticket_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS distinct_actors_avg_6_prior,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY actor)) AS distinct_event_type_in_actor
    FROM rollup_base
    WINDOW w_actor AS (PARTITION BY actor)
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT COUNT(CASE WHEN t.actor = 'agent' THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count
    FROM ranked s
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.actor,
        s.event_type,
        s.ticket_id,
        s.n_actor_customer,
        s.n_event_type_in_resolved_assig,
        s.n_rows,
        s.distinct_ticket_id_in_actor,
        s.n_actor_customer_decile,
        s.distinct_actors_avg_6_prior,
        s.distinct_event_type_in_actor,
        s.event_actor_count,
        t.csat AS ticket_csat,
        t.tags AS ticket_tags,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM summarized_rows s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
    WHERE ARRAY_CONTAINS(t.tags, 'upgrade')
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM combined_details_ticket
    WHERE event_actor_count > (SELECT AVG(event_actor_count) FROM combined_details_ticket)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        actor,
        event_type,
        ticket_id,
        n_actor_customer,
        n_event_type_in_resolved_assig,
        n_rows,
        distinct_ticket_id_in_actor,
        n_actor_customer_decile,
        distinct_actors_avg_6_prior,
        event_actor_count,
        ticket_csat,
        agent_tier,
        RANK() OVER (PARTITION BY actor, agent_team ORDER BY n_event_type_in_resolved_assig, actor, event_type, ticket_id) AS n_event_type_in_resolved_assig_rank,
        ROUND(n_actor_customer / NULLIF(SUM(n_actor_customer) OVER (PARTITION BY n_event_type_in_resolved_assig, ticket_id), 0), 4) AS n_actor_customer_share_of_n_event_type_in_resolv,
        LAG(ticket_csat) OVER (PARTITION BY actor, agent_team ORDER BY n_event_type_in_resolved_assig, actor, event_type, ticket_id) AS prev_ticket_csat
    FROM qualified
)
SELECT
    actor,
    event_type,
    ticket_id,
    n_actor_customer,
    n_event_type_in_resolved_assig,
    n_rows,
    distinct_ticket_id_in_actor,
    n_actor_customer_decile,
    distinct_actors_avg_6_prior,
    ticket_csat,
    agent_tier,
    n_actor_customer_share_of_n_event_type_in_resolv,
    prev_ticket_csat,
    CONCAT_WS('-', event_type, agent_tier) AS event_type_agent_tier_label
FROM metrics
WHERE distinct_actors_avg_6_prior < 7.8
ORDER BY n_actor_customer_share_of_n_event_type_in_resolv DESC
