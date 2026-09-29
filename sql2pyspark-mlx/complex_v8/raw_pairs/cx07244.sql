WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 13:00:00'
      AND event_type IS NOT NULL
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_week,
        t.requester_id AS ticket_requester_id,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM recent s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.requester_id BETWEEN 5 AND 18
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER (PARTITION BY actor, event_type), 0), 4) AS ticket_requester_id_share_of_actor_event_type,
        COUNT(*) OVER (PARTITION BY event_type, YEAR(event_at_week)) AS rows_in_event_type_event_at_week_year,
        DENSE_RANK() OVER w_actor_event_at_week_month_by AS agent_hired_on_dense_rank,
        ROUND((UNIX_TIMESTAMP(event_at_week) - UNIX_TIMESTAMP(LAG(event_at_week) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at_week), event_type ORDER BY agent_hired_on DESC, id))) / 60, 1) AS minutes_since_prev_event_at_week,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY event_type, YEAR(event_at_week)), 2) AS ticket_requester_id_vs_event_type_event_at_week_,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY actor, event_type), 2) AS actor_event_type_avg_ticket_requester_id,
        LAG(event_at, 3) OVER (PARTITION BY ticket_id ORDER BY ticket_requester_id, id) AS prev3_event_at,
        LEAD(event_at_week) OVER (PARTITION BY actor, MONTH(event_at_week) ORDER BY agent_hired_on NULLS LAST, id) AS next_event_at_week,
        RANK() OVER (PARTITION BY actor, MONTH(event_at_week) ORDER BY agent_hired_on NULLS LAST) AS agent_hired_on_rank,
        LAST_VALUE(agent_hired_on) OVER (PARTITION BY ticket_id ORDER BY ticket_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_agent_hired_on,
        NTH_VALUE(event_at_week, 2) OVER (PARTITION BY actor, MONTH(event_at_week) ORDER BY agent_hired_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_week
    FROM enriched
    WINDOW w_actor_event_at_week_month_by AS (PARTITION BY actor, MONTH(event_at_week) ORDER BY agent_hired_on NULLS LAST), w_actor_event_type AS (PARTITION BY actor, event_type)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_week,
        ticket_requester_id,
        agent_hired_on,
        agent_name,
        ticket_requester_id_share_of_actor_event_type,
        rows_in_event_type_event_at_week_year,
        minutes_since_prev_event_at_week,
        ticket_requester_id_vs_event_type_event_at_week_,
        actor_event_type_avg_ticket_requester_id,
        prev3_event_at,
        next_event_at_week,
        agent_hired_on_rank,
        last_agent_hired_on,
        second_event_at_week
    FROM ranked
    WHERE agent_hired_on_dense_rank = 1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_at_week,
        agent_hired_on,
        agent_name,
        minutes_since_prev_event_at_week,
        prev3_event_at,
        next_event_at_week,
        last_agent_hired_on,
        second_event_at_week,
        NTH_VALUE(agent_hired_on_rank, 2) OVER (PARTITION BY MONTH(event_at_week) ORDER BY prev3_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on_rank,
        NTH_VALUE(second_event_at_week, 2) OVER (ORDER BY agent_hired_on DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_event_at_week,
        LEAD(second_event_at_week, 2) OVER (PARTITION BY MONTH(event_at_week) ORDER BY prev3_event_at, id) AS next2_second_event_at_week,
        ROUND(PERCENT_RANK() OVER w_event_type_by_actor_event_ty, 3) AS actor_event_type_avg_ticket_requester_id_percent,
        COUNT(*) OVER (PARTITION BY event_type ORDER BY actor_event_type_avg_ticket_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM leaders
    WINDOW w_event_type_by_actor_event_ty AS (PARTITION BY event_type ORDER BY actor_event_type_avg_ticket_requester_id)
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    agent_name,
    prev3_event_at,
    next_event_at_week,
    last_agent_hired_on,
    second_agent_hired_on_rank,
    second_second_event_at_week,
    next2_second_event_at_week,
    actor_event_type_avg_ticket_requester_id_percent
FROM metrics
ORDER BY actor DESC
