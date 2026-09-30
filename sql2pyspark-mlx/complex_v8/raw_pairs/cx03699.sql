WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE ticket_id <= 33
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_type_actor_label,
        s.actor_prefix,
        s.event_type_len,
        t.csat AS ticket_csat,
        t.opened_at AS ticket_opened_at,
        t.agent_id AS ticket_agent_id,
        su.hired_on AS agent_hired_on
    FROM recent s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
    WHERE t.csat < 3
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(event_type_len, ticket_csat) AS greatest_event_type_len_ticket_csat,
        ADD_MONTHS(agent_hired_on, 12) AS agent_hired_on_plus_12m
    FROM joined
    WHERE agent_hired_on >= DATE '2024-07-09'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_type_actor_label,
        actor_prefix,
        event_type_len,
        ticket_csat,
        ticket_opened_at,
        ticket_agent_id,
        greatest_event_type_len_ticket_csat,
        agent_hired_on_plus_12m,
        DATEDIFF(agent_hired_on_plus_12m, LAG(agent_hired_on_plus_12m) OVER w_agent_hired_on_plus_12m_year) AS days_since_prev_agent_hired_on_plus_12m,
        FIRST_VALUE(ticket_opened_at) OVER (PARTITION BY YEAR(agent_hired_on_plus_12m), ticket_csat ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_opened_at
    FROM enriched
    WINDOW w_agent_hired_on_plus_12m_year AS (PARTITION BY YEAR(agent_hired_on_plus_12m), ticket_csat ORDER BY event_at ASC NULLS LAST, id)
)
SELECT
    ticket_agent_id,
    actor,
    event_type_len,
    SUM(greatest_event_type_len_ticket_csat) AS total_greatest_event_type_len_ticket_csat
FROM metrics
GROUP BY GROUPING SETS ((ticket_agent_id, actor, event_type_len), (ticket_agent_id), (actor))
ORDER BY ticket_agent_id NULLS LAST, actor NULLS LAST, event_type_len NULLS LAST
