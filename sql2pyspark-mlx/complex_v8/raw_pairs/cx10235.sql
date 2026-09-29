WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE actor IN ('system', 'agent')
      AND HOUR(event_at) BETWEEN 7 AND 13
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.days_since_event_at,
        t.tags AS ticket_tags,
        su.hired_on AS agent_hired_on
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER w_ticket_id_by_agent_hired_on) AS days_since_prev_agent_hired_on,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on)) AS rows_in_agent_hired_on_month,
        ROUND(AVG(days_since_event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), actor ORDER BY days_since_event_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS days_since_event_at_avg_3,
        COUNT(*) OVER w_ticket_id_actor AS rows_in_ticket_id_actor,
        LAG(agent_hired_on) OVER (PARTITION BY ticket_id ORDER BY days_since_event_at NULLS LAST, id) AS prev_agent_hired_on,
        LAST_VALUE(days_since_event_at) IGNORE NULLS OVER (PARTITION BY event_type, ticket_id ORDER BY days_since_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY days_since_event_at NULLS LAST, id) AS days_since_event_at_seq,
        LAG(agent_hired_on) OVER (PARTITION BY event_type, ticket_id ORDER BY days_since_event_at, id) AS prev_agent_hired_on_2,
        MAX(days_since_event_at) OVER (PARTITION BY ticket_id, actor) AS ticket_id_actor_max_days_since_event_at,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY ticket_id, actor)) AS distinct_event_type_in_ticket_id_actor,
        ROUND(PERCENT_RANK() OVER w_ticket_id_by_agent_hired_on_2, 3) AS agent_hired_on_percent_rank,
        MIN(days_since_event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on)) AS agent_hired_on_month_min_days_since_event_at,
        ROUND(AVG(days_since_event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on)), 2) AS agent_hired_on_month_avg_days_since_event_at,
        LAG(agent_hired_on) OVER w_agent_hired_on_month_actor_b_2 AS prev_agent_hired_on_3,
        SUM(days_since_event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on)) AS agent_hired_on_month_sum_days_since_event_at
    FROM with_parent_ticket
    WINDOW w_ticket_id_by_agent_hired_on AS (PARTITION BY ticket_id ORDER BY agent_hired_on, id), w_ticket_id_actor AS (PARTITION BY ticket_id, actor), w_ticket_id_by_agent_hired_on_2 AS (PARTITION BY ticket_id ORDER BY agent_hired_on), w_agent_hired_on_month_actor_b_2 AS (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), actor ORDER BY days_since_event_at DESC NULLS FIRST, id)
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    days_since_event_at,
    ticket_tags,
    agent_hired_on,
    rows_in_ticket_id_actor,
    prev_agent_hired_on,
    last_known_days_since_event_at,
    prev_agent_hired_on_2,
    ticket_id_actor_max_days_since_event_at,
    distinct_event_type_in_ticket_id_actor,
    agent_hired_on_percent_rank,
    prev_agent_hired_on_3,
    agent_hired_on_month_sum_days_since_event_at
FROM windowed
ORDER BY agent_hired_on_month_sum_days_since_event_at NULLS LAST, id
LIMIT 10
