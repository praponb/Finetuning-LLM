WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND MONTH(event_at) IN (2, 3)
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.priority AS ticket_priority,
        t.tags AS ticket_tags,
        su.tier AS agent_tier
    FROM base_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.csat BETWEEN 2 AND 5
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_priority, ticket_id ORDER BY event_at_month, id), 3) AS event_at_month_percent_rank,
        ROUND(CUME_DIST() OVER w_ticket_priority_by_event_at, 3) AS event_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(event_at_month) - UNIX_TIMESTAMP(LAG(event_at_month) OVER w_ticket_id_by_event_at)) / 60, 1) AS minutes_since_prev_event_at_month,
        ROW_NUMBER() OVER w_ticket_id_by_event_at AS rn,
        LAG(event_at_month) OVER (ORDER BY event_at_month, id) AS prev_event_at_month,
        NTH_VALUE(event_at_month, 2) OVER (ORDER BY event_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_month,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROW_NUMBER() OVER w_all_by_event_at_month AS event_at_month_seq
    FROM with_parent
    WINDOW w_ticket_priority_by_event_at AS (PARTITION BY ticket_priority ORDER BY event_at, id), w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id), w_all_by_event_at_month AS (ORDER BY event_at_month, id)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_month,
        event_type_actor_label,
        ticket_priority,
        ticket_tags,
        agent_tier,
        event_at_month_percent_rank,
        event_at_cume_dist,
        minutes_since_prev_event_at_month,
        prev_event_at_month,
        second_event_at_month,
        second_event_at,
        event_at_month_seq
    FROM metrics
    WHERE rn <= 3
)
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    event_at_month,
    ticket_tags,
    agent_tier,
    event_at_month_percent_rank,
    event_at_cume_dist,
    minutes_since_prev_event_at_month,
    prev_event_at_month,
    event_at_month_seq,
    CONCAT_WS(': ', agent_tier, actor) AS agent_tier_actor_label,
    CASE WHEN event_type IN ('replied', 'reopened') THEN 'group_a' WHEN event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_type_group
FROM leaders
ORDER BY event_at_month DESC, event_at DESC
