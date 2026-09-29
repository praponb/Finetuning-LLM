WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('|', event_type, actor) AS event_type_actor_label,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-01-31'
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_type_actor_label,
        s.event_type_ref,
        t.opened_at AS ticket_opened_at,
        t.requester_id AS ticket_requester_id,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM recent_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY MONTH(event_at)), 2) AS event_at_month_avg_ticket_requester_id,
        SUM(ticket_requester_id) OVER (PARTITION BY MONTH(event_at)) AS event_at_month_sum_ticket_requester_id,
        LAST_VALUE(ticket_requester_id) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY ticket_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_requester_id,
        ROW_NUMBER() OVER w_all_by_ticket_opened_at AS seq_no,
        SIZE(COLLECT_SET(agent_tier) OVER (PARTITION BY actor)) AS distinct_agent_tier_in_actor,
        RANK() OVER (ORDER BY ticket_opened_at NULLS LAST, id) AS ticket_opened_at_rank,
        SIZE(COLLECT_SET(ticket_id) OVER w_actor) AS distinct_ticket_id_in_actor,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_agent_tier_ticket_opened_at)) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER w_actor, 0), 4) AS ticket_requester_id_share_of_actor,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY actor), 2) AS ticket_requester_id_vs_actor_avg,
        MAX(ticket_requester_id) OVER (PARTITION BY agent_tier, YEAR(ticket_opened_at) ORDER BY event_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ticket_requester_id_max_6_prior,
        MAX(ticket_requester_id) OVER (PARTITION BY actor) AS actor_max_ticket_requester_id,
        LAG(ticket_opened_at) OVER (PARTITION BY agent_tier, YEAR(ticket_opened_at) ORDER BY event_at, id) AS prev_ticket_opened_at
    FROM joined
    WINDOW w_all_by_ticket_opened_at AS (ORDER BY ticket_opened_at NULLS LAST, id), w_actor AS (PARTITION BY actor), w_agent_tier_ticket_opened_at AS (PARTITION BY agent_tier, YEAR(ticket_opened_at) ORDER BY event_at, id)
),
summary AS (
    -- Group-level totals
    SELECT
        ticket_id,
        distinct_agent_tier_in_actor,
        distinct_ticket_id_in_actor,
        SUM(ticket_requester_id_max_6_prior) FILTER (WHERE actor IN ('agent', 'system')) AS ticket_requester_id_max_6_prior_actor_in_agent_s,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        SUM(CASE WHEN actor IS NOT NULL THEN 1 ELSE 0 END) AS n_actor_is_not_null,
        COUNT(*) FILTER (WHERE actor IS NOT NULL) AS n_actor_is_not_null_2
    FROM with_trends
    GROUP BY 1, 2, 3
)
SELECT
    ticket_id,
    distinct_agent_tier_in_actor,
    ticket_requester_id_max_6_prior_actor_in_agent_s,
    actor_list
FROM summary
ORDER BY ticket_requester_id_max_6_prior_actor_in_agent_s, distinct_agent_tier_in_actor NULLS LAST
